import argparse
import io
import sys
import time
import json
import jsonschema

from datetime import datetime
from dateutil import tz
from strict_rfc3339 import rfc3339_to_timestamp
from jsonschema import Draft4Validator, validators
from stitchclient.client import Client
from transit.reader import Reader
from transit.writer import Writer

def parse_headers():
    headers = {'version': sys.stdin.readline().strip().lower()}

    for line in sys.stdin:
        if line.strip() == '--':
            break
        (k,v) = line.split(':', 1)
        headers[k.strip().lower()] = v.strip().lower()

    return headers


def parse_key_fields(full_record):
    return [k for (k,v) in full_record['schema']['properties'].items() if 'key' in v and v['key'] == True]


def parse_record(full_record):
    o = full_record['record']
    v = extend_with_default(Draft4Validator)
    v(full_record['schema'], format_checker=jsonschema.FormatChecker()).validate(o)
    return o


def from_jsonline(args):
    last_bookmark = None
    def persist_bookmark(vals):
        if last_bookmark is not None:
            print(json.dumps(last_bookmark), flush=True)

    with Client(args.cid, args.token, callback_function=persist_bookmark) as stitch:
        while True:
            line = sys.stdin.readline() # `for line in stdin` won't play nice in all situations
            if line == '':
                break
            o = json.loads(line)

            if o['type'] == 'RECORD':
                key_fields = parse_key_fields(o)
                parsed_record = parse_record(o)
                stitch.push({'action': 'upsert',
                             'table_name': o['stream'],
                             'key_names': key_fields,
                             'sequence': int(time.time() * 1000),
                             'data': parsed_record}, last_bookmark)
            elif o['type'] == 'BOOKMARK':
                last_bookmark = o['value']
            else:
                pass

    
def extend_with_default(validator_class):
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for error in validate_properties(
            validator, properties, instance, schema,
        ):
            yield error

        for property, subschema in properties.items():
            if "format" in subschema:
                if subschema['format'] == 'date-time':
                    instance[property] = datetime.fromtimestamp(
                        rfc3339_to_timestamp(instance[property])
                    ).replace(tzinfo=tz.tzutc())

    return validators.extend(
        validator_class, {"properties" : set_defaults},
    )


def main(args):
    parser = argparse.ArgumentParser(prog="Stitch Persister")
    parser.add_argument('-T', '--token', required=True, help='Stitch API token')
    parser.add_argument('-C', '--cid', required=True, type=int, help='Stitch Client ID')
    args = parser.parse_args(args)

    headers = parse_headers()

    if headers['content-type'] == "jsonline":
        from_jsonline(args)
    else:
        raise RuntimeError("Unsupported content-type: {}".format(headers['content-type']))

if __name__ == '__main__':
    schema = {
        'type':'object',
        'properties':{
            'id':{'type':'int', 'key':True},
            'ts':{'type':'string','format':'date-time'},
            'max_int':{'type': 'integer'},
            'max_float':{'type': 'number'},
            'obj':{'type': 'object',
                   'properties': {
                       'nested_date': {'type':'string', 'format':'date-time'}
                   }
               }
    },'required':['ts', 'max_int', 'max_float']}
#    print(parse_key_fields({'schema': schema}))
    obj = json.loads('{"ts":"2016-01-01T00:00:00Z", "max_int":9223372036854775807, "max_float": 9223372036854775807.9223372036854775807, "obj":{"nested_date":"2017-01-01T00:00:00Z"}}')
    v = extend_with_default(Draft4Validator)
    v(schema, format_checker=jsonschema.FormatChecker()).validate(obj)
    print(obj)
