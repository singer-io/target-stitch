from  decimal import Decimal
from datetime import datetime
import dateutil
import pytz
from target_stitch.exceptions import BatchTooLargeException
import singer
import time
import json

LOGGER = singer.get_logger().getChild('target_stitch')

#above 6, sequence ids will eclipse the maximum value of a LONG
MAX_SEQUENCE_SUFFIX_LENGTH=6

#the gate will NOT accept POST which contain more than 20k records
MAX_NUM_GATE_RECORDS=20000
#the gate will NOT accept POST which contain more than 4MB
MAX_NUM_GATE_BYTES=(4 * 1024 * 1024)

def ensure_multipleof_is_decimal(schema):
    '''Ensure multipleOf (if exists) points to a Decimal.

        Recursively walks the given schema (which must be dict), converting
        every instance of the multipleOf keyword to a Decimal.

        This modifies the input schema and also returns it.

        '''
    if 'multipleOf' in schema:
        schema['multipleOf'] = Decimal(str(schema['multipleOf']))

    if 'properties' in schema:
        for k, v in schema['properties'].items():
            ensure_multipleof_is_decimal(v)

    if 'items' in schema:
        ensure_multipleof_is_decimal(schema['items'])

    return schema

def marshall_data(schema, data, schema_predicate, data_marshaller):
    if schema is None:
        return data

    if "properties" in schema and isinstance(data, dict):
        for key, subschema in schema["properties"].items():
            if key in data:
                data[key] = marshall_data(subschema, data[key], schema_predicate, data_marshaller)
        return data

    if "items" in schema and isinstance(data, list):
        subschema = schema["items"]
        for i in range(len(data)):
            data[i] = marshall_data(subschema, data[i], schema_predicate, data_marshaller)
        return data

    if schema_predicate(schema, data):
        return data_marshaller(data)

    return data

def marshall_date_times(schema, data):
    return marshall_data(
        schema, data,
        lambda s, d: "format" in s and s["format"] == 'date-time' and isinstance(d, str),
        lambda d: strptime(d))

def marshall_decimals(schema, data):
    return marshall_data(
        schema, data,
        lambda s, d: "multipleOf" in s and isinstance(d, (float, int)),
        lambda d: Decimal(str(d)))


def generate_sequence(message_num):
    '''Generates a unique sequence number based on the current time millis
        with a zero-padded message number based on the magnitude of max_records.'''
    sequence_base = str(int(time.time() * 1000))
    sequence_suffix = str(message_num).zfill(MAX_SEQUENCE_SUFFIX_LENGTH)

    return int(sequence_base + sequence_suffix)

def determine_table_version(first_message):
    if first_message and first_message.version is not None:
        return first_message.version
    return None



def serialize_gate_messages(messages, schema, key_names, bookmark_names):
    '''Produces request bodies for Stitch.

    Builds a request body consisting of all the messages. Serializes it as
    JSON. If the result exceeds the request size limit, splits the batch
    in half and recurs.
    '''

    serialized_messages = []
    for idx, message in enumerate(messages):
        if isinstance(message, singer.RecordMessage):
            record_message = {
                'action': 'upsert',
                'data': message.record,
                'sequence': generate_sequence(idx)
            }

            if message.time_extracted:
                record_message['time_extracted'] = singer.utils.strftime(message.time_extracted)

            serialized_messages.append(record_message)
        elif isinstance(message, singer.ActivateVersionMessage):
            serialized_messages.append({
                'action': 'activate_version',
                'sequence': generate_sequence(idx)
            })

    body = {
        'table_name': messages[0].stream,
        'schema': schema,
        'key_names': key_names,
        'messages': serialized_messages
    }
    if determine_table_version(messages[0]):
        body['table_version'] = determine_table_version(messages[0])

    if bookmark_names:
        body['bookmark_names'] = bookmark_names

    # We are not using Decimals for parsing here. We recognize that
    # exposes data to potential rounding errors. However, the Stitch API
    # as it is implemented currently is also subject to rounding errors.
    # This will affect very few data points and we have chosen to leave
    # conversion as is for now.

    serialized = json.dumps(body)
    LOGGER.debug('Serialized %d messages into %d bytes', len(messages), len(serialized))

    if len(serialized) < MAX_NUM_GATE_BYTES:
        return [serialized]

    if len(messages) <= 1:
        raise BatchTooLargeException(
            "A single record is larger than the Stitch API limit of {} Mb".format(
                MAX_NUM_GATE_BYTES // (1024 * 1024)))

    pivot = len(messages) // 2
    l_half = serialize_gate_messages(messages[:pivot], schema, key_names, bookmark_names)
    r_half = serialize_gate_messages(messages[pivot:], schema, key_names, bookmark_names)
    return l_half + r_half


def strptime_format1(d):
    return datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC)

def strptime_format2(d):
    return datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)

def strptime_format_fallback(d):
    return dateutil.parser.parse(d)

def strptime(d):
    """
    The target should only encounter date-times consisting of the formats below.
    They are compatible with singer-python's strftime function.
    """
    for fn in [strptime_format1, strptime_format2, strptime_format_fallback]:
        try:
            return fn(d)
        except:
            pass
    raise Exception("Got an unparsable datetime: {}".format(d))
