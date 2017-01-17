import argparse
import logging
import os
import sys
import time
import json
import jsonschema

from datetime import datetime
from dateutil import tz
from strict_rfc3339 import rfc3339_to_timestamp
from jsonschema import Draft4Validator, validators
from stitchclient.client import Client

logger = logging.getLogger()
schema_cache = {}
last_state = None

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
                    instance[property] = datetime.fromtimestamp(
                        rfc3339_to_timestamp(instance[property])
                    ).replace(tzinfo=tz.tzutc())

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
    o = full_record['record']
    v = extend_with_default(Draft4Validator)
    v(schema, format_checker=jsonschema.FormatChecker()).validate(o)
    return o


def push_state(vals):
    logger.info('Persisted {} records to Stitch'.format(len(vals)))
    if last_state is not None:
        print("I would persist this state " + json.dumps(last_state))

        
def persist_line(stitchclient, line):
    global schema_cache, last_state
    o = json.loads(line)
    if o['type'] == 'RECORD':
        key_fields = parse_key_fields(o['stream'])
        parsed_record = parse_record(o)
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


def main(args):
    configure_logging()
    with Client(
            int(os.environ['STITCH_CLIENT_ID']),
            os.environ['STITCH_TOKEN'],
            callback_function=push_state
    ) as stitchclient:
        while True:
            line = proc.stdout.readline()
            line = line.decode('utf-8')
            if line == '' or line is None:
                break
            persist_line(stitchclient, line)


            
if __name__ == '__main__':
    main(sys.argv[1:])
