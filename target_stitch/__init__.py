#!/usr/bin/env python3

import argparse
from collections import namedtuple
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

StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties'])

class Batch(object):
    def __init__(self, table_name, table_version):
        self.table_name = table_name
        self.table_version = table_version
        self.key_names = []
        self.records = []
        self.size = 0
        self.activate_version = False


class GateClient(object):
    def __init__(self):
        self.session = requests.Session()

    def send_batch(self, batch):
        msg = { }
        msg['table_name'] = batch.table_name
        if self.table_version:
            msg['table_version'] = batch.table_version
        if self.bookmark_key and batch.bookmark_value:
            msg['bookmark'] = {
                'key': batch.bookmark_key,
                'value': batch.bookmark_value
            }
        msg['records'] = copy.copy(batch.records)
        msg['extraction_started_at'] = batch.extraction_started_at
        self.session.post(self.gate_url, msg)

class TargetStitch(object):

    def __init__(self, gate_client, state_writer):
        self.batch = None
        self.state = None
        self.stream_meta = {}
        self.gate_client = gate_client
        self.state_writer = state_writer
        self.max_batch_bytes = 4000000
        self.max_batch_records = 20000

    def flush_to_gate(self):
        self.gate_client.send_batch(self.batch)
        self.batch = None

    def flush_state(self):
        line = json.dumps(self.state)
        logger.debug('Emitting state {}'.format(line))
        self.state_writer.write("{}\n".format(line))
        self.state_writer.flush()
        self.state = None

    def flush(self):
        if self.batch:
            logger.info('Flushing batch of {} records'.format(len(self.batch.records)))
            self.flush_to_gate()
            self.flush_state()

    def flush_if_new_table(self, table_name, table_version):
        if self.batch:
            if (table_name == self.batch.table_name and
                table_version == self.batch.table_version):
                return
            else:
                self.flush()
                self.batch = Batch(table_name, table_version)
        else:
            self.batch = Batch(table_name, table_version)

    def handle_line(self, line):
        message = singer.parse_message(line)
        result = None

        # If we got a Schema, set the schema and key properties for
        # this stream. Flush the batch, if there is one, under the
        # assumption that subsequent messages will relate to the new
        # schema.
        if isinstance(message, singer.SchemaMessage):
            self.stream_meta[message.stream] = StreamMeta(
                message.schema, message.key_properties)
            self.flush()

        elif isinstance(message, singer.RecordMessage):
            self.flush_if_new_table(message.stream, message.version)
            if self.batch.size + len(line) > self.max_batch_bytes:
                self.flush()
            self.batch.records.append(message.record)
            self.batch.size += len(line)

        elif isinstance(message, singer.ActivateVersionMessage):
            self.flush_if_new_table(message.stream, message.version)
            self.batch.activate_version = True

        elif isinstance(message, singer.StateMessage):
            self.state = message.value

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.flush()


class DryRunClient(GateClient):
    """A client that doesn't actually persist to the Gate.

    Useful for testing.
    """

    def __init__(self):
        self.output_file = '/tmp/stitch-target-out.json'
        try:
            os.remove(self.output_file)
        except OSError:
            pass

    def send_batch(self, batch):
        logger.info("---- DRY RUN: NOTHING IS BEING PERSISTED TO STITCH ----")
        with open(self.output_file, 'a') as outfile:
            for m in batch.records:
                logger.info("---- DRY RUN: WOULD HAVE SENT: %s", batch.table_name)
                json.dump(m, outfile)
                outfile.write('\n')


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
            return Client(client_id, token, stitch_url=url)
        else:
            return Client(client_id, token)


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
    client = stitch_client(args)
    with TargetStitch(client, sys.stdout) as target_stitch:
        for line in input:
            target_stitch.handle_line(line)

    logger.info("Exiting normally")

if __name__ == '__main__':
    main()
