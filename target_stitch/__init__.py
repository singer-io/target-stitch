#!/usr/bin/env python3

# Changes target-stitch makes to schema and record
# ================================================
#
# We have to make several changes to both the schema and the record in
# order to correctly transform datatypes from the JSON input to the
# Transit output and in order to take advantage of JSON Schema validation.
#
# 1. We walk the schema and look for instances of `"multipleOf": x` and we
# convert x to a Decimal. We do this because in the validation step below,
# both multipleOf and the value need to be Decimal.
#
# 2. We walk each record along with its schema and look for cases where
# the schema says `"multipleOf": x` and the value in the record is a
# float or int.
#
# 3. We then validate the record against the schema using the jsonschema library.
#
# 4. After validation, we walk the record and the schema again looking for
# nodes with type string and format date-time. We convert all such values
# to datetime. We need to do this _after_ validation because validation
# operates on strings, not datetime objects.

import argparse
from datetime import datetime
from decimal import Decimal
import http.client
import io
import json
import os
import sys
import threading
import time
import urllib

import pkg_resources

import singer

logger = singer.get_logger()


def number_to_decimal(x):
    '''Converts float or int to Decimal.'''

    # Need to call str on it first because Decimal(int) will lose
    # precision
    return Decimal(str(x))


class Batch:

    def __init__(self,
                 table_name,
                 bookmark_key=None,
                 bookmark_value=None):
        self.table_name = None
        self.records = []

    def add_record(self, record):
        self.records.append({'data': record})

    def serialize(self):
        result = {}
        result['table_name'] = self.table_name
        if self.table_version:
            result['table_version'] = self.table_version
        if self.bookmark_key and self.bookmark_value:
            result['bookmark'] = {
                'key': self.bookmark_key,
                'value': self.bookmark_value
            }
        result['records'] = self.records
        result['extraction_started_at'] = self.extraction_started_at
        return json.dumps(result)


class TargetStitch(object):

    def __init__(self):
        self.current_table = None
        self.current_table_version = None
        self.state = None
        self.records = []
        self.batch_size = 0
        self.stream_meta = {}

    def clear(self):
        self.records.clear()
        self.batch_size = 0
        self.current_table = None
        self.current_table_version = None

    def flush_if_new_table(self, table_name, table_version):
        if (table_name != current_table_name or
            table_version != self.current_table_version):
            self.flush()
            self.current_table = table_name
            self.current_table_version = table_version

    def handle_line(self, line):
        message = singer.parse_message(line)

        # If we got a Schema, set the schema and key properties for
        # this stream. Flush the batch, if there is one, under the
        # assumption that subsequent messages will relate to the new
        # schema.
        if isinstance(message, singer.SchemaMessage):
            self.stream_meta[message.stream] = StreamMeta(
                message.schema, message.key_properties)
            if self.current_table:
                self.flush()

        elif isinstance(message, singer.RecordMessage):
            self.flush_if_new_table(message.table_name, message.version)
            if self.batch_size + len(line) > self.max_batch_size:
                self.flush()
            self.records.append(message.record)
            self.batch_size += len(line)

        elif isinstance(message, singer.ActivateVersionMessage):
            self.flush_if_new_table(message.table_name, message.version)
            self.activate_table_version = True

        elif isinstance(message, singer.StateMessage):
            self.state = message.state

    def flush_to_gate(self):
        msg = {}
        msg['table_name'] = self.table_name
        if self.table_version:
            msg['table_version'] = self.table_version
        if self.bookmark_key and self.bookmark_value:
            msg['bookmark'] = {
                'key': self.bookmark_key,
                'value': self.bookmark_value
            }
        msg['records'] = self.records
        msg['extraction_started_at'] = self.extraction_started_at
        return json.dumps(msg)


    def flush(self):

        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()
        logger.info('Persisted batch of {} records to Stitch'.format(len(states)))


class TransitDumper(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            # wanted a simple yield str(o) in the next line,
            # but that would mean a yield on the line with super(...),
            # which wouldn't work (see my comment below), so...
            return str(obj)
        else:
            return json.JSONEncoder.default(self, obj)


class DryRunTarget(StitchTarget):
    """A client that doesn't actually persist to the Gate.

    Useful for testing.
    """

    def __init__(self, buffer_size=100):
        self.pending_callback_args = []
        self.buffer_size = buffer_size
        self.pending_messages = []
        self.output_file = '/tmp/stitch-target-out.json'
        try:
            os.remove(self.output_file)
        except OSError:
            pass

    def flush(self):
        logger.info("---- DRY RUN: NOTHING IS BEING PERSISTED TO STITCH ----")
        write_last_state(self.pending_callback_args)
        self.pending_callback_args = []
        with open(self.output_file, 'a') as outfile:
            for m in self.pending_messages:
                logger.info("---- DRY RUN: WOULD HAVE SENT: %s %s", m.get('action'), m.get('table_name'))
                json.dump(m, outfile, cls=TransitDumper)
                outfile.write('\n')
            self.pending_messages = []

    def push(self, message, callback_arg=None):
        self.pending_callback_args.append(callback_arg)
        self.pending_messages.append(message)

        if len(self.pending_callback_args) % self.buffer_size == 0:
            self.flush()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.flush()


def persist_lines(stitchclient, lines):
    """Takes a client and a stream and persists all the records to the gate,
    printing the state to stdout after each batch."""
    state = None
    stream_meta = {}

    for line in lines:
        target_stitch.handle_line(line)


def stitch_client(args):
    """Returns an instance of StitchClient or DryRunClient"""
    if args.dry_run:
        return DryRunClient()
    else:
        with open(args.config) as input:
            config = json.load(input)

        if not config.get('disable_collection', False):
            logger.info('Sending version information to stitchdata.com. ' +
                        'To disable sending anonymous usage data, set ' +
                        'the config parameter "disable_collection" to true')
            threading.Thread(target=collect).start()

        missing_fields = []

        if 'client_id' in config:
            client_id = config['client_id']
        else:
            missing_fields.append('client_id')

        if 'token' in config:
            token = config['token']
        else:
            missing_fields.append('token')

        if missing_fields:
            raise Exception('Configuration is missing required fields: {}'
                            .format(missing_fields))

        if 'stitch_url' in config:
            url = config['stitch_url']
            logger.debug("Persisting to Stitch Gate at {}".format(url))
            return Client(client_id, token, callback_function=write_last_state, stitch_url=url)
        else:
            return Client(client_id, token, callback_function=write_last_state)


def collect():
    try:
        version = pkg_resources.get_distribution('target-stitch').version
        conn = http.client.HTTPSConnection('collector.stitchdata.com', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-stitch',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    parser.add_argument('-n', '--dry-run', help='Dry run - Do not push data to Stitch', action='store_true')
    args = parser.parse_args()

    if not args.dry_run and args.config is None:
        parser.error("config file required if not in dry run mode")

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    with stitch_client(args) as client:
        state = persist_lines(client, input)
    write_last_state([state])
    logger.info("Exiting normally")

if __name__ == '__main__':
    main()
