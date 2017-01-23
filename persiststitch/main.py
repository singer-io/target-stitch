import argparse
import atexit
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
dry_run = False
state_file = None
last_state = None
row_count = 0

def configure_logging(level=logging.DEBUG):
    global logger
    logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
                if subschema['format'] == 'date-time' and property in instance:
                    try:
                        instance[property] = datetime.fromtimestamp(
                            rfc3339_to_timestamp(instance[property])
                        ).replace(tzinfo=tz.tzutc())
                    except Exception as e:
                        raise Exception('Error parsing property {}, value {}'.format(
                            property, instance[property]))

    return validators.extend(
        validator_class, {"properties" : set_defaults},
    )



def parse_key_fields(stream_name):
    if stream_name in schema_cache and 'properties' in schema_cache[stream_name]:
        return [k for (k,v) in schema_cache[stream_name]['properties'].items() if 'key' in v and v['key'] == True]
    else:
        return []

    
def parse_record(full_record):
    stream_name = full_record['stream']
    schema = schema_cache[stream_name] if stream_name in schema_cache else {}
    o = copy.deepcopy(full_record['record'])
    v = extend_with_default(Draft4Validator)
    v(schema, format_checker=FormatChecker()).validate(o)
    return o


def push_state(vals):
    global state_file, last_state

    if vals is not None:
        logger.info('Persisted {} records to Stitch'.format(len(vals)))

    if last_state is not None:
        if state_file is not None:
            state = json.dumps(last_state)
            logger.info("Persisting state to file " + state)
            state_file.seek(0)
            state_file.write(state)
        else:
            logger.info("I would persist this state " + json.dumps(last_state))

        
def persist_line(stitchclient, line):
    global dry_run, schema_cache, last_state, state_file, row_count
    o = json.loads(line)
    if o['type'] == 'RECORD':
        key_fields = parse_key_fields(o['stream'])
        try:
            parsed_record = parse_record(o)
        except:
            raise Exception("Error parsing record {}".format(o))
        row_count += 1
        if state_file is not None and row_count % 100 == 0:
            push_state(None)
        if dry_run:
            logger.info('Dry Run - Would persist one record')
        else:
            stitchclient.push({'action': 'upsert',
                               'table_name': o['stream'],
                               'key_names': key_fields,
                               'sequence': int(time.time() * 1000),
                               'data': parsed_record}, last_state)
    elif o['type'] == 'STATE':
        last_state = o['value']
    elif o['type'] == 'SCHEMA':
        schema_cache[o['stream']] = o['schema']
    else:
        pass


def main():
    global dry_run, state_file, last_state
    parser = argparse.ArgumentParser()
    parser.add_argument('-dry',
                        '--dry-run',
                        help='Dry run - Do not push data to Stitch',
                        dest='dry_run',
                        action='store_true')
    parser.add_argument('--state-file',
                        help='Store state to a local file')

    args = parser.parse_args()

    if args.dry_run:
        dry_run = True

    if args.state_file:
        state_file = open(args.state_file, 'w')

    configure_logging()
    with Client(
            int(os.environ['STITCH_CLIENT_ID']),
            os.environ['STITCH_TOKEN'],
            callback_function=push_state
    ) as stitchclient:
        input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
        for line in input_stream:
            if line == '' or line is None:
                break
            persist_line(stitchclient, line)

    def exit_handler():
        if state_file is not None:
            state_file.seek(0)
            state_file.write(json.dumps(last_state))
            state_file.truncate()
            state_file.close()

    atexit.register(exit_handler)

            
if __name__ == '__main__':
    main()
