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

import dateutil

from jsonschema import ValidationError, Draft4Validator, FormatChecker
from jsonschema.exceptions import SchemaError
from stitchclient.client import Client
import singer

logger = singer.get_logger()


def number_to_decimal(x):
    '''Converts float or int to Decimal.'''

    # Need to call str on it first because Decimal(int) will lose
    # precision
    return Decimal(str(x))


class SchemaKey:
    '''Constants representing keywords in JSON Schema'''
    multipleOf = 'multipleOf'
    properties = 'properties'
    items = 'items'
    format = 'format'
    type = 'type'


def ensure_multipleof_is_decimal(schema):
    '''Ensure multipleOf (if exists) points to a Decimal.

    Recursively walks the given schema (which must be dict), converting
    every instance of the multipleOf keyword to a Decimal.

    This modifies the input schema and also returns it.

    '''
    if SchemaKey.multipleOf in schema:
        schema[SchemaKey.multipleOf] = number_to_decimal(schema[SchemaKey.multipleOf])

    if SchemaKey.properties in schema:
        for k, v in schema[SchemaKey.properties].items():
            ensure_multipleof_is_decimal(v)

    if SchemaKey.items in schema:
        ensure_multipleof_is_decimal(schema[SchemaKey.items])

    return schema

def correct_numeric_types(schema, data):
    '''Convert numeric values that should be Decimals into Decimals.

    Recursively walks the schema along with the data. For every node where
    the schema is "number" and there is a "multipleOf" specified and the
    value is a float or int, converts the value to a Decimal.

    This modifies the input data and also returns it.
    '''
    if schema is None:
        return data

    if SchemaKey.properties in schema and isinstance(data, dict):
        for key, subschema in schema[SchemaKey.properties].items():
            if key in data:
                data[key] = correct_numeric_types(subschema, data[key])
        return data

    if SchemaKey.items in schema and isinstance(data, list):
        subschema = schema[SchemaKey.items]
        for i in range(len(data)):
            data[i] = correct_numeric_types(subschema, data[i])
        return data

    if SchemaKey.multipleOf in schema and isinstance(data, (float, int)):
        return number_to_decimal(data)

    # True and False are instances of bool and also instances of int. I
    # don't think it's appropriate to accept true and false JSON values as
    # numbers though. So don't attempt to translate True and False to
    # float. Let the validation step reject them later.
    if schema.get(SchemaKey.type) == 'number' and isinstance(data, int) and not isinstance(data, bool):
        return float(data)

    return data

def convert_datetime_strings_to_datetime(schema, data):
    '''Convert string values that should be datetimes into datetimes.

    Recursively walks the schema along with the data. For every node where
    the schema is "string" and there the "format" is "date-time" and the
    value is a string, converts the value to a datetime using
    dateutil.parser.parse. We use that parser because it correctly handles
    the date-time formats required by JSON Schema.

    This modifies the input data and also returns it.
    '''
    if schema is None:
        return data

    if SchemaKey.properties in schema and isinstance(data, dict):
        for key, subschema in schema[SchemaKey.properties].items():
            if key in data:
                data[key] = convert_datetime_strings_to_datetime(subschema, data[key])
        return data

    if SchemaKey.items in schema and isinstance(data, list):
        subschema = schema[SchemaKey.items]
        for i in range(len(data)):
            data[i] = convert_datetime_strings_to_datetime(subschema, data[i])
        return data

    if SchemaKey.format in schema and schema[SchemaKey.format] == 'date-time' and isinstance(data, str):
        return dateutil.parser.parse(data)

    return data


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


class DryRunClient(object):
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


def parse_record(stream, record, schemas, validators):
    '''Parses the data out of a record message.

    Converts numbers to decimals for fields where the schema indicates
    decimal precision via multipleOf.

    Converts strings to datetimes for fields where the schema indicates
    "format": "date-time".

    Raises a ValueError if the resulting record does not pass validation.

    '''
    if stream in schemas:
        schema = schemas[stream]
    else:
        schema = {}

    # We need to correct numeric types (convert floats and ints to
    # decimals, convert ints to floats) where appropriate before doing
    # validation, because validation requires that the multipleOf value in
    # the schema and the value in the record have the same type.
    record = correct_numeric_types(schema, record)

    validator = validators[stream]

    try:
        validator.validate(record)
    except ValidationError as exc:
        raise ValueError('Record({}) does not conform to schema. Please see logs for details.{}'.format(stream,record)) from exc

    # We need to convert strings to datetimes after validation because
    # validation operates on strings, not datetimes.
    return convert_datetime_strings_to_datetime(schema, record)

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
                'data': parse_record(message.stream, message.record, schemas, validators)
            }
            if message.version is not None:
                stitch_message['table_version'] = message.version

            stitchclient.push(stitch_message, state)
            state = None

        elif isinstance(message, singer.ActivateVersionMessage):
            stitch_message = {
                'action': 'switch_view',
                'table_name': message.stream,
                'table_version': message.version,
                'sequence': int(time.time() * 1000)
            }
            stitchclient.push(stitch_message, state)
            state = None

        elif isinstance(message, singer.StateMessage):
            state = message.value

        elif isinstance(message, singer.SchemaMessage):

            # Draft4Validator will fail unless both the multipleOf in the
            # schema and the value in the data are Decimal. So we need to
            # convert all instances of multipleOf in the schema to
            # Decimal.
            schemas[message.stream] = ensure_multipleof_is_decimal(message.schema)
            key_properties[message.stream] = message.key_properties

            # JSON schema will complain if 'required' is present but
            # empty, so don't set it if there are no key properties
            if message.key_properties:
                schemas[message.stream]['required'] = message.key_properties

            try:
                Draft4Validator.check_schema(message.schema)
            except SchemaError as schema_error:
                raise Exception("Invalid json schema for stream {}: {}".format(message.stream, message.schema)) from schema_error

            validators[message.stream] = Draft4Validator(message.schema, format_checker=FormatChecker())

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
