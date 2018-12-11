import singer
import backoff
import requests
import boto
from boto.s3.key import Key
import time
import json
import io
import os
import sys
import pytz
import uuid
import hashlib

from transit.writer import Writer
from datetime import datetime
from decimal import Decimal
from requests.exceptions import RequestException, HTTPError
from target_stitch.timings import Timings
from target_stitch.exceptions import TargetStitchException
from jsonschema import SchemaError, ValidationError, Draft4Validator, FormatChecker

LOGGER = singer.get_logger().getChild('target_stitch')
MESSAGE_VERSION=2
PIPELINE_VERSION='2'
STITCH_SPOOL_URL = "{}/spool/private/v1/clients/{}/batches"

TIMINGS = Timings()

def now():
    return singer.utils.strftime(singer.utils.now())

def _log_backoff(details):
    (_, exc, _) = sys.exc_info()
    LOGGER.info(
        'Error sending data to Stitch. Sleeping %d seconds before trying again: %s',
        details['wait'], exc)

def strptime(d):
    """
    The target should only encounter date-times consisting of the formats below.
    They are compatible with singer-python's strftime function.
    """
    try:
        rtn = datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ")
    except:
        rtn = datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%fZ")

    return rtn.replace(tzinfo=pytz.UTC)

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


def determine_table_version(first_message):
    if first_message and first_message.version is not None:
        return first_message.version
    return None

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


def transit_encode(pipeline_messages):
    with TIMINGS.mode('transit_encode'):
        with io.BytesIO() as buf:
            writer = Writer(buf, "msgpack")
            for m in pipeline_messages:
                writer.write(m)
            data = buf.getvalue()
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

class StitchHandler: # pylint: disable=too-few-public-methods
    '''Sends messages to Stitch.'''

    def __init__(self, token, client_id, connection_ns, stitch_url, spool_host, spool_s3_bucket, max_batch_bytes, max_batch_records):
        self.token = token
        self.client_id = client_id
        self.connection_ns = connection_ns
        self.stitch_url = stitch_url
        self.spool_host = spool_host
        self.session = requests.Session()
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_records = max_batch_records
        self.s3_conn = boto.connect_s3()
        self.bucket_name = spool_s3_bucket
        self.bucket = self.s3_conn.get_bucket(self.bucket_name)
        self.send_methods = {}

    def post_to_s3(self, data, num_records, table_name):
        key_name = self.generate_s3_key(data)
        k = Key(self.bucket)
        k.key = key_name
        LOGGER.info("Sending batch with %d messages for table %s to s3 %s",
                    num_records, table_name, key_name)

        with TIMINGS.mode('post_to_s3'):
            start_persist = time.time()
            k.set_contents_from_string(data)
            persist_time = int((time.time() - start_persist) * 1000)

        return (key_name, persist_time)

    def generate_sequence(self, message_num):
        '''Generates a unique sequence number based on the current time millis
        with a zero-padded message number based on the magnitude of max_records.'''
        sequence_base = str(int(time.time() * 1000))

        # add an extra order of magnitude to account for the fact that we can
        # actually accept more than the max record count
        fill = len(str(10 * self.max_batch_records))
        sequence_suffix = str(message_num).zfill(fill)

        return int(sequence_base + sequence_suffix)

    def serialize_s3_upsert_messages(self, records, schema, table_name, key_names ):
        pipeline_messages = []
        for idx, msg in enumerate(records):
            with TIMINGS.mode('marshall_date_times'):
                marshalled_msg = marshall_date_times(schema, msg)
                pipeline_messages.append({'message_version' : MESSAGE_VERSION,
                                          'pipeline_version' : PIPELINE_VERSION,
                                          "timestamps" : {"_rjm_received_at" :  int(time.time() * 1000)},
                                          'body' : {
                                              'client_id' : int(self.client_id),
                                              'namespace' : self.connection_ns,
                                              'table_name' : table_name,
                                              'action'     : 'upsert',
                                              'sequence'   : self.generate_sequence(idx),
                                              'key_names'  : key_names,
                                              'data': msg
                                          }})
        return pipeline_messages

    def serialize_gate_messages(self, messages, schema, key_names, bookmark_names, max_bytes, max_records):
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
                    'sequence': self.generate_sequence(idx)
                }

                if message.time_extracted:
                    record_message['time_extracted'] = singer.utils.strftime(message.time_extracted)

                serialized_messages.append(record_message)
            elif isinstance(message, singer.ActivateVersionMessage):
                serialized_messages.append({
                    'action': 'activate_version',
                    'sequence': self.generate_sequence(idx)
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

        if len(serialized) < max_bytes:
            return [serialized]

        if len(messages) <= 1:
            raise BatchTooLargeException(
                "A single record is larger than the Stitch API limit of {} Mb".format(
                    max_bytes // 1000000))

        pivot = len(messages) // 2
        l_half = serialize(messages[:pivot], schema, key_names, bookmark_names, max_bytes, max_records)
        r_half = serialize(messages[pivot:], schema, key_names, bookmark_names, max_bytes, max_records)
        return l_half + r_half

    def generate_s3_key(self, data):
        return  "{:07d}/{}-{}-{}".format(
            int(self.client_id),
            uuid.uuid4(),
            hashlib.sha1(data).hexdigest(),
            singer.utils.strftime(singer.utils.now(), "%Y%m%d-%H%M%S%f")
        )

    def headers(self):
        '''Return the headers based on the token'''
        return {
            'Authorization': 'Bearer {}'.format(self.token),
            'Content-Type': 'application/json'
        }

    @backoff.on_exception(backoff.expo,
                          RequestException,
                          giveup=singer.utils.exception_is_4xx,
                          max_tries=8,
                          on_backoff=_log_backoff)
    def send(self, data):
        '''Send the given data to Stitch, retrying on exceptions'''
        ssl_verify = os.environ.get("TARGET_STITCH_SSL_VERIFY") != 'false'
        response = self.session.post(self.stitch_url,
                                     headers=self.headers(),
                                     data=data,
                                     verify=ssl_verify)
        response.raise_for_status()
        return response

    @backoff.on_exception(backoff.expo,
                          RequestException,
                          giveup=singer.utils.exception_is_4xx,
                          max_tries=8,
                          on_backoff=_log_backoff)
    def post_to_spool(self, body):
        '''Send the given data to the spool, retrying on exceptions'''
        ssl_verify = os.environ.get("TARGET_STITCH_SSL_VERIFY") != 'false'
        response = self.session.post(STITCH_SPOOL_URL.format(self.spool_host, self.client_id),
                                     headers=self.headers(),
                                     json=body,
                                     verify=ssl_verify)
        response.raise_for_status()
        return response

    def handle_batch(self, messages, buffer_size_bytes, schema, key_names, bookmark_names=None):
        table_name = messages[0].stream
        self.handle_s3(messages, schema, key_names, bookmark_names=None)

        # if table_name not in self.send_methods:
        #     if (buffer_size_bytes >= (4 * 1024 * 1024)) or len(messages) > self.max_batch_records:
        #         self.send_methods[table_name] = 's3'
        #     else:
        #         self.send_methods[table_name] = 'gate'

        # if (self.send_methods.get(table_name) == 's3'):
        #     self.handle_s3(messages, schema, key_names, bookmark_names=None)
        # else:
        #     self.handle_gate(messages, schema, key_names, bookmark_names=None)

        TIMINGS.log_timings()

    def handle_s3_upserts(self, messages, schema, key_names, bookmark_names=None):
        LOGGER.info("handling batch of %s upserts for table %s to s3", len(messages), messages[0].stream)
        table_name = messages[0].stream

        table_version = determine_table_version(messages[0])
        num_records = len(messages)

        schema = ensure_multipleof_is_decimal(schema)
        #try putting bullshit schema
        validator = Draft4Validator(schema, format_checker=FormatChecker())

        #NB> Decimal marshalling must occur BEFORE schema validation
        with TIMINGS.mode('marshall_decimals'):
            records_with_decimals = [marshall_decimals(schema, m.record) for m in messages]

        with TIMINGS.mode('validate_records'):
            for msg in records_with_decimals:
                try:
                    validator.validate(msg)
                except ValidationError as exc:
                    raise ValueError('Record({}) does not conform to schema. Please see logs for details.'
                                     .format(rec)) from exc
                except SchemaError as exc:
                    raise ValueError('Schema({}) is invalid. Please see logs for details. {}'
                                     .format(schema)) from exc

        if bookmark_names:
            # We only support one bookmark key
            bookmark_key = bookmark_names[0]
            bookmarks = [r[bookmark_key] for r in records_with_decimals]
            bookmark_min = min(bookmarks)
            bookmark_max = max(bookmarks)
            bookmark_metadata = [{
                "key": bookmark_key,
                "min_value": bookmark_min,
                "max_value": bookmark_max,
            }]
        else:
            bookmark_metadata = None

        # TODO: add _sdc fields to records?
        pipeline_messages = self.serialize_s3_upsert_messages(records_with_decimals, schema, table_name, key_names)

        data = transit_encode(pipeline_messages)
        key_name, persist_time = self.post_to_s3(data, num_records, table_name)

        with TIMINGS.mode('post_to_spool'):
            body = {
                "namespace"    : self.connection_ns,
                "table_name"   : table_name,
                "table_version": table_version,
                "action": "upsert",
                "max_time_extracted": now(),
                "bookmark_metadata": bookmark_metadata,
                "s3_key": key_name,
                "s3_bucket": self.bucket_name,
                "num_records": num_records,
                "num_bytes": len(data),
                "format": "transit+msgpack",
                "format_version": "0.8.281",
                "persist_duration_millis": persist_time,
            }
            self.post_to_spool(body)


    def handle_s3_activate_version(self, messages, schema, key_names, bookmark_names=None):
        LOGGER.info("handling activate_version for table %s to s3", messages[0].stream)
        table_name = messages[0].stream
        table_version = determine_table_version(messages[0])
        pipeline_message = {
            "message_version" : MESSAGE_VERSION,
            "pipeline_version" :  PIPELINE_VERSION,
            "timestamps" : {"_rjm_received_at" : int(time.time() * 1000)},
            "body" : {"client_id" : self.client_id,
                      "namespace" : "perftest",
                      "table_name" : table_name,
                      "action" : "switch_view",
                      "sequence" : self.generate_sequence(1)}
            }
        data = transit_encode([pipeline_message])

        key_name, persist_time = self.post_to_s3(data, num_records, table_name)

        with TIMINGS.mode('post_to_spool'):
            body = {
                "namespace"    : self.connection_ns,
                "table_name"   : table_name,
                "table_version": table_version,
                "action": "switch_view",
                "max_time_extracted": now(),
                "bookmark_metadata": None,
                "s3_key": key_name,
                "s3_bucket": self.bucket_name,
                "num_records": 1,
                "num_bytes": len(data),
                "format": "transit+msgpack",
                "format_version": "0.8.281",
                "persist_duration_millis": persist_time,
            }
            self.post_to_spool(body)


    def handle_s3(self, messages, schema, key_names, bookmark_names=None):
        activate_versions = []
        upserts = []

        for msg in messages:
            if isinstance(msg, singer.ActivateVersionMessage):
                activate_versions.append(msg)
            elif isinstance(msg, singer.RecordMessage):
                upserts.append(msg)
            else:
                raise Exception('unrecognized message type')

        if upserts:
            self.handle_s3_upserts(upserts, schema, key_names, bookmark_names)
        if activate_versions:
            self.handle_s3_activate_version(activate_versions, schema, key_names, bookmark_names)


    def handle_gate(self, messages, schema, key_names, bookmark_names=None):
        '''Handle messages by sending them to Stitch.

        If the serialized form of the messages is too large to fit into a
        single request this will break them up into multiple smaller
        requests.
        '''

        LOGGER.info("Sending batch with %d messages for table %s to %s",
                    len(messages), messages[0].stream, self.stitch_url)
        with TIMINGS.mode('serializing'):
            bodies = self.serialize_gate_messages(messages,
                                             schema,
                                             key_names,
                                             bookmark_names,
                                             self.max_batch_bytes,
                                             self.max_batch_records)

        LOGGER.debug('Split batch into %d requests', len(bodies))
        for i, body in enumerate(bodies):
            with TIMINGS.mode('post_to_gate'):
                LOGGER.debug('Request %d of %d is %d bytes', i + 1, len(bodies), len(body))
                try:
                    response = self.send(body)
                    LOGGER.debug('Response is %s: %s', response, response.content)

                # An HTTPError means we got an HTTP response but it was a
                # bad status code. Try to parse the "message" from the
                # json body of the response, since Stitch should include
                # the human-oriented message in that field. If there are
                # any errors parsing the message, just include the
                # stringified response.
                except HTTPError as exc:
                    try:
                        response_body = exc.response.json()
                        if isinstance(response_body, dict) and 'message' in response_body:
                            msg = response_body['message']
                        elif isinstance(response_body, dict) and 'error' in response_body:
                            msg = response_body['error']
                        else:
                            msg = '{}: {}'.format(exc.response, exc.response.content)
                    except: # pylint: disable=bare-except
                        LOGGER.exception('Exception while processing error response')
                        msg = '{}: {}'.format(exc.response, exc.response.content)

                    raise TargetStitchException('Error persisting data for table "{}": {}'.format(
                                                messages[0].stream, msg))

                # A RequestException other than HTTPError means we
                # couldn't even connect to stitch. The exception is likely
                # to be very long and gross. Log the full details but just
                # include the summary in the critical error message. TODO:
                # When we expose logs to Stitch users, modify this to
                # suggest looking at the logs for details.
                except RequestException as exc:
                    LOGGER.exception(exc)
                    raise TargetStitchException('Error connecting to Stitch')
