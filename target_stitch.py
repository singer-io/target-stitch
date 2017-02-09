#!/usr/bin/env python3

import argparse
import logging
import logging.config
import os
import copy
import io
import sys
import time
import json
from datetime import datetime
from dateutil import tz

from strict_rfc3339 import rfc3339_to_timestamp

from jsonschema import Draft4Validator, validators, FormatChecker
from stitchclient.client import Client
import stitchstream

logger = stitchstream.get_logger()


class DryRunClient(object):
    """A client that doesn't actually persist to the Gate.

    Useful for testing.
    """

    def __init__(self, callback_function):
        self.callback_function = callback_function
        self.pending_callback_args = []

    def flush(self):
        logger.info("---- DRY RUN: NOTHING IS BEING PERSISTED TO STITCH ----")
        self.callback_function(self.pending_callback_args)
        self.pending_callback_args = []

    def push(self, message, callback_arg=None):
        self.pending_callback_args.append(callback_arg)

        if len(self.pending_callback_args) % 100 == 0:
            self.flush()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.flush()


def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for error in validate_properties(
            validator, properties, instance, schema,
        ):
            yield error

        for property, subschema in properties.items():
            if "format" in subschema:
                if (subschema['format'] == 'date-time' and
                        property in instance and
                        instance[property] is not None):
                    try:
                        instance[property] = datetime.fromtimestamp(
                            rfc3339_to_timestamp(instance[property])
                        ).replace(tzinfo=tz.tzutc())
                    except Exception as e:
                        raise Exception('Error parsing property {}, value {}'
                                        .format(property, instance[property]))

    return validators.extend(
        validator_class, {"properties": set_defaults},
    )


def parse_key_fields(stream_name, schemas):
    if (stream_name in schemas and
            'properties' in schemas[stream_name]):
        return [k for (k, v) in schemas[stream_name]['properties'].items()
                if 'key' in v and v['key'] is True]
    else:
        return []


def parse_record(full_record, schemas):
    try:
        stream_name = full_record['stream']
        if stream_name in schemas:
            schema = schemas[stream_name]
        else:
            schema = {}
        o = copy.deepcopy(full_record['record'])
        v = extend_with_default(Draft4Validator)
        v(schema, format_checker=FormatChecker()).validate(o)
        return o
    except:
        raise Exception("Error parsing record {}".format(full_record))


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def push_state(states):
    """Called with the list of states associated with the messages that we
just persisted to the gate. These states will often be None. Finds the
last non-None state and emits it. We only need to emit the last one.

    """
    logger.info('Persisted batch of {} records to Stitch'.format(len(states)))
    last_state = None
    for state in states:
        if state is not None:
            last_state = state
    emit_state(last_state)


def persist_lines(stitchclient, lines):
    """Takes a client and a stream and persists all the records to the gate,
printing the state to stdout after each batch."""
    state = None
    schemas = {}
    key_properties = {}
    for line in lines:
        o = json.loads(line)

        if 'type' not in o:
            raise Exception("Line is missing required key 'type': {}".format(line))
        t = o['type']

        if t == 'RECORD':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            message = {'action': 'upsert',
                       'table_name': stream,
                       'key_names': key_properties[stream],
                       'sequence': int(time.time() * 1000),
                       'data': parse_record(o, schemas)}
            stitchclient.push(message, state)
            state = None
        elif t == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = o['stream']
            schemas[stream] = o['schema']
            if 'key_properties' not in o:
                raise Exception("key_properties field is required")
            key_properties[stream] = o['key_properties']
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    return state


def stitch_client(args):
    """Returns an instance of StitchClient or DryRunClient"""
    if args.dry_run:
        return DryRunClient(callback_function=push_state)
    else:
        with open(args.config) as input:
            config = json.load(input)
        missing_fields = []

        if 'client_id' in config:
            client_id = config['client_id']
        else:
            missing_fields.append('client_id')

        if 'token' in config:
            token = config['token']
        else:
            missing_fields.append('token')

        if len(missing_fields) > 0:
            raise Exception('Configuration is missing required fields: {}'
                            .format(missing_fields))
        if 'stitch_url' in config:
            url = config['stitch_url']
            logger.info("Persisting to Stitch Gate at {}".format(url))
            return Client(client_id, token,
                          callback_function=push_state,
                          stitch_url=url)
        else:
            return Client(client_id, token,
                          callback_function=push_state)

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument('-n',
                        '--dry-run',
                        help='Dry run - Do not push data to Stitch',
                        action='store_true')

    args = parser.parse_args()

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = None
    with stitch_client(args) as client:
        state = persist_lines(client, input)
    emit_state(state)
    logger.debug("Exiting normally")



if __name__ == '__main__':
    main()
