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
import requests
import copy
import gzip
from datetime import datetime, timezone

import pkg_resources

import singer

logger = singer.get_logger()

StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties'])

class Batch(object):
    def __init__(self, table_name, table_version, schema, key_names):
        self.table_name = table_name
        self.table_version = table_version
        # TODO: Add vintage to singer spec and change taps to emit it.
        # TODO: Taps should emit the sequence number also
        self.vintage = datetime.now(timezone.utc).isoformat()
        self.schema = schema
        self.key_names = key_names
        self.messages = []
        self.size = 0


DEFAULT_STITCH_URL = 'https://api.stitchdata.com/v2/import/batch'
        
class StitchHandler(object):
    def __init__(self, token, stitch_url=DEFAULT_STITCH_URL):
        self.session = requests.Session()
        self.token = token
        self.stitch_url = stitch_url

    def send_batch(self, body):
        headers = {
            'Authorization': 'Bearer {}'.format(self.token),
            'Content-Type': 'application/json'}
        resp = self.session.post(self.stitch_url, headers=headers, json=body)
        resp.raise_for_status()        


class DryRunClient(StitchHandler):
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
        body = self._request_body(batch)
        with open(self.output_file, 'a') as outfile:
            logger.info("---- DRY RUN: Would have sent batch with %d messages for table %s", len(batch.messages), batch.table_name)
            json.dump(body, outfile, indent=2)


def request_body(batch):
    msg = { }
    msg['table_name'] = batch.table_name
    if batch.table_version:
        msg['table_version'] = batch.table_version
    if batch.schema:
        msg['schema'] = batch.schema
    msg['messages'] = copy.copy(batch.messages)
    msg['vintage'] = batch.vintage

    return msg


class TargetStitch(object):

    def __init__(self, gate_client, state_writer):
        self.batch = None
        self.state = None

        # Mapping from stream name to {'schema': ..., 'key_names': ...}
        self.stream_meta = {}

        # Instance of StitchHandler
        self.gate_client = gate_client

        # Writer that we write state records to
        self.state_writer = state_writer

        # Batch size limits. Stored as properties here so we can easily
        # change for testing.
        self.max_batch_bytes = 4000000
        self.max_batch_records = 20000

    def flush_to_gate(self):
        body = request_body(self.batch)
        self.gate_client.send_batch(body)
        self.batch = None

    def flush_state(self):
        if self.state:
            line = json.dumps(self.state)
            logger.debug('Emitting state {}'.format(line))
            self.state_writer.write("{}\n".format(line))
            self.state_writer.flush()
            self.state = None

    def flush(self):
        if self.batch:
            logger.info('Flushing batch of {} messages'.format(len(self.batch.messages)))
            self.flush_to_gate()
            self.flush_state()

    def flush_if_new_table(self, stream, version):
        if self.batch:
            if (stream == self.batch.table_name and
                version == self.batch.table_version):
                return
            else:
                self.flush()
        self.ensure_batch(stream, version)

    def ensure_batch(self, stream, version):
        stream_meta = self.stream_meta[stream]
        self.batch = Batch(stream, version, stream_meta.schema, stream_meta.key_properties)

    def handle_line(self, line):
        '''Takes a raw line from stdin and handles it, updating state and possibly
        flushing the batch to the Gate and the state to the output stream.'''

        message = singer.parse_message(line)

        # If we got a Schema, set the schema and key properties for this
        # stream. Flush the batch, if there is one, in case the schema is
        # different.
        if isinstance(message, singer.SchemaMessage):
            self.stream_meta[message.stream] = StreamMeta(
                message.schema, message.key_properties)
            self.flush()

        elif isinstance(message, (singer.RecordMessage, singer.ActivateVersionMessage)):
            self.flush_if_new_table(message.stream, message.version)
            if (self.batch.size + len(line) > self.max_batch_bytes):
                self.flush()
                self.ensure_batch(message.stream, message.version)

            if isinstance(message, singer.RecordMessage):
                self.batch.messages.append({
                    'action': 'upsert',
                    'data': message.record,
                    'sequence': int(time.time() * 1000)})
            elif isinstance(message, singer.ActivateVersionMessage):
                self.batch.messages.append({
                    'action': 'activate_version',
                    'sequence': int(time.time() * 1000)})
            self.batch.size += len(line)
            if len(self.batch.messages) >= self.max_batch_records:
                self.flush()

        elif isinstance(message, singer.StateMessage):
            self.state = message.value

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.flush()


def stitch_client(args):
    """Returns an instance of StitchHandler or DryRunClient"""
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

        if 'token' in config:
            token = config['token']
        else:
            missing_fields.append('token')

        if missing_fields:
            raise Exception('Configuration is missing required fields: {}'
                            .format(missing_fields))

        kwargs = {}
        if 'stitch_url' in config:
            logger.info("Persisting to Stitch at {}".format(config['stitch_url']))
            kwargs['stitch_url'] = config['stitch_url']
        if 'gzip_requests' in config:
            logger.info('gzip requests: ' + str(config['gzip_requests']))
            kwargs['gzip_requests'] = config['gzip_requests']

        return StitchHandler(token, **kwargs)


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
