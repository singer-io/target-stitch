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
import singer

logger = singer.get_logger()


def write_last_state(states):
    logger.info('Persisted batch of {} records to Stitch'.format(len(states)))
    last_state = None
    for state in reversed(states):
        if state is not None:
            last_state = state
            break
    if last_state:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


class DryRunClient(object):
    """A client that doesn't actually persist to the Gate.

    Useful for testing.
    """

    def __init__(self, buffer_size=100):
        self.pending_callback_args = []
        self.buffer_size = buffer_size


    def flush(self):
        logger.info("---- DRY RUN: NOTHING IS BEING PERSISTED TO STITCH ----")
        write_last_state(self.pending_callback_args)
        self.pending_callback_args = []

    def push(self, message, callback_arg=None):
        self.pending_callback_args.append(callback_arg)

        if len(self.pending_callback_args) % self.buffer_size == 0:
            self.flush()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.flush()


def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for error in validate_properties(validator, properties, instance, schema):
            yield error

        for property, subschema in properties.items():
            if "format" in subschema:
                if subschema['format'] == 'date-time' and instance.get(property) is not None:
                    try:
                        instance[property] = datetime.fromtimestamp(
                            rfc3339_to_timestamp(instance[property])
                        ).replace(tzinfo=tz.tzutc())
                    except Exception as e:
                        raise Exception('Error parsing property {}, value {}'
                                        .format(property, instance[property]))

    return validators.extend(validator_class, {"properties": set_defaults})


def parse_record(stream, record, schemas):
    if stream in schemas:
        schema = schemas[stream]
    else:
        schema = {}
    o = copy.deepcopy(record)
    v = extend_with_default(Draft4Validator)
    v(schema, format_checker=FormatChecker()).validate(o)
    return o


def persist_lines(stitchclient, lines):
    """Takes a client and a stream and persists all the records to the gate,
    printing the state to stdout after each batch."""
    state = None
    schemas = {}
    key_properties = {}
    for line in lines:

        message = singer.parse_message(line)

        if isinstance(message, singer.RecordMessage):
            stitch_message = {
                'action': 'upsert',
                'table_name': message.stream,
                'key_names': key_properties[message.stream],
                'sequence': int(time.time() * 1000),
                'data': parse_record(message.stream, message.record, schemas)}
            stitchclient.push(stitch_message, state)
            state = None

        elif isinstance(message, singer.StateMessage):
            state = message.value

        elif isinstance(message, singer.SchemaMessage):
            schemas[message.stream] = message.schema
            key_properties[message.stream] = message.key_properties

        else:
            raise Exception("Unrecognized message {} parsed from line {}".format(message, line))

    return state


def stitch_client(args):
    """Returns an instance of StitchClient or DryRunClient"""
    if args.dry_run:
        return DryRunClient()
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

        if missing_fields:
            raise Exception('Configuration is missing required fields: {}'
                            .format(missing_fields))

        if 'stitch_url' in config:
            url = config['stitch_url']
            logger.debug("Persisting to Stitch Gate at {}".format(url))
            return Client(client_id, token, callback_function=write_last_state, stitch_url=url)
        else:
            return Client(client_id, token, callback_function=write_last_state)


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
