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
from decimal import Decimal
from datetime import datetime, timezone

from jsonschema import ValidationError, Draft4Validator, FormatChecker

import pkg_resources

import singer

logger = singer.get_logger()

StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties'])

MAX_BATCH_BYTES = 6000

DEFAULT_STITCH_URL = 'https://api.stitchdata.com/v2/import/batch'

class BatchTooLargeException(Exception):
    pass

class StitchHandler(object):
    def __init__(self, token, stitch_url, max_batch_bytes):
        self.token = token
        self.stitch_url = stitch_url
        self.session = requests.Session()
        self.max_batch_bytes = max_batch_bytes

    def handle_batch(self, messages, schema, key_names):

        headers = {
            'Authorization': 'Bearer {}'.format(self.token),
            'Content-Type': 'application/json'}

        logger.info("Sending batch with %d messages for table %s to %s", len(messages), messages[0].stream, self.stitch_url)
        bodies = serialize(messages, schema, key_names, self.max_batch_bytes)

        for i, body in enumerate(bodies):
            logger.info("Request body %d is %d bytes", i, len(body))
            resp = self.session.post(self.stitch_url, headers=headers, data=body)
            if resp.status_code // 100 == 2:
                logger.info('Ok')
            else:
                raise Exception('%s: %s', resp, resp.content)


class LoggingHandler(object):

    def __init__(self, output_file, max_batch_bytes):
        self.output_file = output_file
        self.max_batch_bytes = max_batch_bytes

    def handle_batch(self, messages, schema, key_names):
        logger.info("Saving batch with %d messages for table %s to %s",
                    len(messages), messages[0].stream, self.output_file.name)
        for i, body in enumerate(serialize(messages, schema, key_names, self.max_batch_bytes)):
            logger.info("  Request body %d is %d bytes", i, len(body))
            json.dump(body, self.output_file)
            self.output_file.write('\n')


def float_to_decimal(x):
    if isinstance(x, float):
        return Decimal(str(x))
    elif isinstance(x, list):
        return [float_to_decimal(child) for child in x]
    elif isinstance(x, dict):
        return {k: float_to_decimal(v) for k, v in x.items()}
    else:
        return x

class ValidatingHandler(object):

    def handle_batch(self, messages, schema, key_names):
        schema = float_to_decimal(schema)
        validator = Draft4Validator(schema, format_checker=FormatChecker())
        for message in messages:
            if isinstance(message, singer.RecordMessage):
                data = float_to_decimal(message.record)
                validator.validate(data)
        logger.info('Batch is valid')


def serialize(messages, schema, key_names, max_bytes):

    serialized_messages = []
    for message in messages:
        if isinstance(message, singer.RecordMessage):
            serialized_messages.append({
                'action': 'upsert',
                'data': message.record,
                'vintage': datetime.now(timezone.utc).isoformat(),
                'sequence': int(time.time() * 1000)})
        elif isinstance(message, singer.ActivateVersionMessage):
            serialized_messages.append({
                'action': 'activate_version',
                'sequence': int(time.time() * 1000)})

    body = {
        'table_name': messages[0].stream,
        'schema': schema,
        'key_names': key_names,
        'messages': serialized_messages
    }
    if messages[0].version is not None:
        body['table_version'] = messages[0].version

    serialized = json.dumps(body)
    logger.info('Serialized %d messages into %d bytes', len(messages), len(serialized))

    if len(serialized) < max_bytes:
        return [serialized]

    n = len(messages)
    if n <= 1:
        raise BatchTooLargeException("A single record is larger than batch size limit of %d")

    pivot = n // 2
    l_half = messages[:pivot]
    r_half = messages[pivot:]
    return serialize(l_half, schema, key_names, max_bytes) + serialize(r_half, schema, key_names, max_bytes)


class TargetStitch(object):

    def __init__(self, handlers, state_writer):
        self.messages = []
        self.state = None

        # Mapping from stream name to {'schema': ..., 'key_names': ...}
        self.stream_meta = {}

        # Instance of StitchHandler
        self.handlers = handlers

        # Writer that we write state records to
        self.state_writer = state_writer

        # Batch size limits. Stored as properties here so we can easily
        # change for testing.
        self.max_batch_records = 20000


    def flush(self):
        if self.messages:
            logger.info('Flushing batch of {} messages'.format(len(self.messages)))
            stream_meta = self.stream_meta[self.messages[0].stream]
            for handler in self.handlers:
                handler.handle_batch(self.messages, stream_meta.schema, stream_meta.key_properties)
            self.messages = []

        if self.state:
            line = json.dumps(self.state)
            logger.debug('Emitting state {}'.format(line))
            self.state_writer.write("{}\n".format(line))
            self.state_writer.flush()
            self.state = None

    def handle_line(self, line):
        '''Takes a raw line from stdin and handles it, updating state and possibly
        flushing the batch to the Gate and the state to the output stream.'''

        message = singer.parse_message(line)

        # If we got a Schema, set the schema and key properties for this
        # stream. Flush the batch, if there is one, in case the schema is
        # different.
        if isinstance(message, singer.SchemaMessage):
            self.flush()
            self.stream_meta[message.stream] = StreamMeta(
                message.schema, message.key_properties)

        elif isinstance(message, (singer.RecordMessage, singer.ActivateVersionMessage)):
            if self.messages and (
                    message.stream != self.messages[0].stream or
                    message.version != self.messages[0].version):
                self.flush()
            self.messages.append(message)
            if len(self.messages) >= self.max_batch_records:
                self.flush()

        elif isinstance(message, singer.StateMessage):
            self.state = message.value

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.flush()


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
    parser.add_argument('-c', '--config', help='Config file', type=argparse.FileType('r'))
    parser.add_argument('-n', '--dry-run', help='Dry run - Do not push data to Stitch', action='store_true')
    parser.add_argument('-o', '--output-file', help='Save requests to this output file', type=argparse.FileType('w'))
    args = parser.parse_args()

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    handlers = []
    if args.output_file:
        handlers.append(LoggingHandler(args.output_file, MAX_BATCH_BYTES))
    if args.dry_run:
        handlers.append(ValidatingHandler())
    elif not args.config:
        parser.error("config file required if not in dry run mode")
    else:
        config = json.load(args.config)
        token = config.get('token')
        stitch_url = config.get('stitch_url', DEFAULT_STITCH_URL)

        if not token:
            raise Exception('Configuration is missing required "token" field')

        if not config.get('disable_collection'):
            logger.info('Sending version information to stitchdata.com. ' +
                        'To disable sending anonymous usage data, set ' +
                        'the config parameter "disable_collection" to true')
            threading.Thread(target=collect).start()
        handlers.append(StitchHandler(token=token, stitch_url=stitch_url, max_batch_bytes=MAX_BATCH_BYTES))

    with TargetStitch(handlers, sys.stdout) as target_stitch:
        for line in input:
            target_stitch.handle_line(line)

    logger.info("Exiting normally")

if __name__ == '__main__':
    main()
