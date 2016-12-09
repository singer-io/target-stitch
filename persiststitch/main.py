import argparse
import io
import logging
import os
import sys
import time
import json
import jsonschema
import asyncio
import asyncio.subprocess

from datetime import datetime
from dateutil import tz
from strict_rfc3339 import rfc3339_to_timestamp
from jsonschema import Draft4Validator, validators
from stitchclient.client import Client

MAX_LINE_SIZE=4*1024*1024 # 4mb limit
logger = logging.getLogger()
schema_cache = {}
last_bookmark = None

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


def parse_headers(raw_headers):
    headers = {'version': raw_headers.pop(0).strip().lower()}

    for line in raw_headers:
        (k,v) = line.split(':', 1)
        headers[k.strip().lower()] = v.strip().lower()

    return headers


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


def print_bookmark(vals):
    logger.info('Persisted {} records to Stitch'.format(len(vals)))
    if last_bookmark is not None:
        print(json.dumps(last_bookmark), flush=True)

        
def from_jsonline(stitchclient, line):
    global schema_cache, last_bookmark
    o = json.loads(line)
    if o['type'] == 'RECORD':
        key_fields = parse_key_fields(o['stream'])
        parsed_record = parse_record(o)
        stitchclient.push({'action': 'upsert',
                           'table_name': o['stream'],
                           'key_names': key_fields,
                           'sequence': int(time.time() * 1000),
                           'data': parsed_record}, last_bookmark)
    elif o['type'] == 'BOOKMARK':
        last_bookmark = o['value']
    elif o['type'] == 'SCHEMA':
        schema_cache[o['stream']] = o['schema']
    else:
        pass

async def run_subprocess(stitchclient, args):
    create = asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.PIPE,
        limit=MAX_LINE_SIZE
    )
    proc = await create
    logger.info('Subprocess started {}'.format(proc.pid))
    
    raw_headers = []
    
    while True:
        line = await proc.stdout.readline() 
        if line.decode('utf-8').strip() == '--':
            break
        raw_headers.append(line.decode('utf-8'))

    headers = parse_headers(raw_headers)

    if headers['content-type'] == 'jsonline':
        persist_fn = from_jsonline
    else:
        raise RuntimeError('Unsupported content-type: {}'.format(headers['content-type']))

    while True:
        line = await proc.stdout.readline()
        line = line.decode('utf-8')
        if line == '' or line is None:
            break
        persist_fn(stitchclient, line)
            
    logger.info('Waiting for process {} to complete'.format(proc.pid))
    await proc.wait()

    return_code = proc.returncode
    logger.info('Subprocess {} returned code {}'.format(proc.pid, return_code))

    return return_code


def main(args):
    configure_logging()
    with Client(
            int(os.environ['STITCH_CLIENT_ID']),
            os.environ['STITCH_TOKEN'],
            callback_function=print_bookmark
    ) as stitchclient:
        event_loop = asyncio.get_event_loop()
        try:
            return_code = event_loop.run_until_complete(
                run_subprocess(stitchclient, args)
            )
        finally:
            event_loop.close()

            
if __name__ == '__main__':
    main(sys.argv[1:])
