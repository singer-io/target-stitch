import argparse
import logging
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

logger = logging.getLogger()
schema_cache = {}


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


def configure_logging(level=logging.DEBUG):
    global logger
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


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


def parse_key_fields(stream_name):
    if (stream_name in schema_cache and
            'properties' in schema_cache[stream_name]):
        return [k for (k, v) in schema_cache[stream_name]['properties'].items()
                if 'key' in v and v['key'] is True]
    else:
        return []


def parse_record(full_record):
    try:
        stream_name = full_record['stream']
        if stream_name in schema_cache:
            schema = schema_cache[stream_name]
        else:
            schema = {}
        o = copy.deepcopy(full_record['record'])
        v = extend_with_default(Draft4Validator)
        v(schema, format_checker=FormatChecker()).validate(o)
        return o
    except:
        raise Exception("Error parsing record {}".format(full_record))


# TODO: Get review on state saving part
def push_state(states):
    logger.info('Persisted batch of {} records to Stitch'.format(len(states)))
    for state in states:
        if state is not None:
            logger.debug('Emitting state {}'.format(state))
            print(state)


def persist_lines(stitchclient, lines):
    global schema_cache
    state = None
    for line in lines:
        o = json.loads(line)
        if o['type'] == 'RECORD':
            message = {'action': 'upsert',
                       'table_name': o['stream'],
                       'key_names': parse_key_fields(o['stream']),
                       'sequence': int(time.time() * 1000),
                       'data': parse_record(o)}
            stitchclient.push(message, state)
            state = None
        elif o['type'] == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif o['type'] == 'SCHEMA':
            schema_cache[o['stream']] = o['schema']
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))
    return state


def stitch_client(dry_run):
    if dry_run:
        return DryRunClient(
            callback_function=push_state)
    else:
        return Client(
            int(os.environ['STITCH_CLIENT_ID']),
            os.environ['STITCH_TOKEN'],
            callback_function=push_state)


def main():
    global dry_run
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--dry-run',
                        help='Dry run - Do not push data to Stitch',
                        dest='dry_run',
                        action='store_true')
    parser.add_argument('--state-file',
                        help='Store state to a local file')

    args = parser.parse_args()

    if args.dry_run:
        dry_run = True

    configure_logging()

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = None
    with stitch_client(dry_run) as client:
        state = persist_lines(client, input)
    if state is not None:
        logger.debug('Emitting final state {}'.format(state))
        print(state)


if __name__ == '__main__':
    main()
