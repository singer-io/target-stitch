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
import threading
import http.client
import urllib
import pkg_resources

from datetime import datetime
from dateutil import tz

from strict_rfc3339 import rfc3339_to_timestamp

from jsonschema import ValidationError, Draft4Validator, validators, FormatChecker
from jsonschema.exceptions import SchemaError
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
                        instance[property] = datetime.utcfromtimestamp(
                            rfc3339_to_timestamp(instance[property])
                        ).replace(tzinfo=tz.tzutc())
                    except Exception as e:
                        raise Exception('Error parsing property {}, value {}'
                                        .format(property, instance[property]))

    return validators.extend(validator_class, {"properties": set_defaults})


def parse_record(stream, record, schemas, validators):
    if stream in schemas:
        schema = schemas[stream]
    else:
        schema = {}
    o = copy.deepcopy(record)
    validator = validators[stream]
    try:
        validator.validate(o)
    except ValidationError as exc:
        raise ValueError('Record does not conform to schema. Please see logs for details.') from exc
    return o


def persist_lines(stitchclient, lines):
    """Takes a client and a stream and persists all the records to the gate,
    printing the state to stdout after each batch."""
    state = None
    schemas = {}
    key_properties = {}
    validators = {}
    for line in lines:
        message = singer.parse_message(line)

        if isinstance(message, singer.RecordMessage):
            if message.stream not in key_properties:
                raise Exception("Missing schema for {}".format(message.stream))

            stitch_message = {
                'action': 'upsert',
                'table_name': message.stream,
                'key_names': key_properties[message.stream],
                'sequence': int(time.time() * 1000),
                'data': parse_record(message.stream, message.record, schemas, validators)}
            stitchclient.push(stitch_message, state)
            state = None

        elif isinstance(message, singer.StateMessage):
            state = message.value

        elif isinstance(message, singer.SchemaMessage):
            schemas[message.stream] = message.schema
            key_properties[message.stream] = message.key_properties

            # JSON schema will complain if 'required' is present but
            # empty, so don't set it if there are no key properties
            if message.key_properties:
                schemas[message.stream]['required'] = message.key_properties
            validator = extend_with_default(Draft4Validator)

            try:
                validator.check_schema(message.schema)
            except SchemaError as schema_error:
                raise Exception("Invalid json schema for stream {}: {}".format(message.stream, message.schema)) from schema_error

            validators[message.stream] = validator(message.schema, format_checker=FormatChecker())

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
