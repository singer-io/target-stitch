#!/usr/bin/env python3
# pylint: disable=too-many-arguments,invalid-name,too-many-nested-blocks

'''
Target for Stitch API.
'''

import argparse
import copy
import gzip
import http.client
import io
import json
import os
import re
import sys
import time
import urllib

from threading import Thread
from contextlib import contextmanager
from collections import namedtuple
from datetime import datetime, timezone
from decimal import Decimal, getcontext
import psutil

import requests
from requests.exceptions import RequestException, HTTPError
from jsonschema import ValidationError, Draft4Validator, FormatChecker
import simplejson
import pkg_resources
import singer
import backoff

LOGGER = singer.get_logger().getChild('target_stitch')

# We use this to store schema and key properties from SCHEMA messages
StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])

DEFAULT_STITCH_URL = 'https://api.stitchdata.com/v2/import/batch'
DEFAULT_MAX_BATCH_BYTES = 4000000
DEFAULT_MAX_BATCH_RECORDS = 20000
SEQUENCE_MULTIPLIER = 1000

class TargetStitchException(Exception):
    '''A known exception for which we don't need to print a stack trace'''
    pass

class MemoryReporter(Thread):
    '''Logs memory usage every 30 seconds'''

    def __init__(self):
        self.process = psutil.Process()
        super().__init__(name='memory_reporter', daemon=True)

    def run(self):
        while True:
            LOGGER.debug('Virtual memory usage: %.2f%% of total: %s',
                         self.process.memory_percent(),
                         self.process.memory_info())
            time.sleep(30.0)


class Timings:
    '''Gathers timing information for the three main steps of the Tap.'''
    def __init__(self):
        self.last_time = time.time()
        self.timings = {
            'serializing': 0.0,
            'posting': 0.0,
            None: 0.0
        }

    @contextmanager
    def mode(self, mode):
        '''We wrap the big steps of the Tap in this context manager to accumulate
        timing info.'''

        start = time.time()
        yield
        end = time.time()
        self.timings[None] += start - self.last_time
        self.timings[mode] += end - start
        self.last_time = end


    def log_timings(self):
        '''We call this with every flush to print out the accumulated timings'''
        LOGGER.debug('Timings: unspecified: %.3f; serializing: %.3f; posting: %.3f;',
                     self.timings[None],
                     self.timings['serializing'],
                     self.timings['posting'])

TIMINGS = Timings()


class BatchTooLargeException(TargetStitchException):
    '''Exception for when the records and schema are so large that we can't
    create a batch with even one record.'''
    pass


def _log_backoff(details):
    (_, exc, _) = sys.exc_info()
    LOGGER.info(
        'Error sending data to Stitch. Sleeping %d seconds before trying again: %s',
        details['wait'], exc)


class StitchHandler: # pylint: disable=too-few-public-methods
    '''Sends messages to Stitch.'''

    def __init__(self, token, stitch_url, max_batch_bytes, max_batch_records):
        self.token = token
        self.stitch_url = stitch_url
        self.session = requests.Session()
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_records = max_batch_records

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
        url = self.stitch_url
        headers = self.headers()
        ssl_verify = True
        if os.environ.get("TARGET_STITCH_SSL_VERIFY") == 'false':
            ssl_verify = False

        response = self.session.post(url, headers=headers, data=data, verify=ssl_verify)
        response.raise_for_status()
        return response


    def handle_batch(self, messages, schema, key_names, bookmark_names=None):
        '''Handle messages by sending them to Stitch.

        If the serialized form of the messages is too large to fit into a
        single request this will break them up into multiple smaller
        requests.

        '''

        LOGGER.info("Sending batch with %d messages for table %s to %s",
                    len(messages), messages[0].stream, self.stitch_url)
        with TIMINGS.mode('serializing'):
            bodies = serialize(messages,
                               schema,
                               key_names,
                               bookmark_names,
                               self.max_batch_bytes,
                               self.max_batch_records)

        LOGGER.debug('Split batch into %d requests', len(bodies))
        for i, body in enumerate(bodies):
            with TIMINGS.mode('posting'):
                LOGGER.debug('Request %d of %d is %d bytes', i + 1, len(bodies), len(body))
                try:
                    response = self.send(body)
                    LOGGER.debug('Response is {}: {}'.format(response, response.content))

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
                    raise TargetStitchException('Error persisting data for table ' +
                                                '"' + messages[0].stream +'": ' +
                                                msg)

                # A RequestException other than HTTPError means we
                # couldn't even connect to stitch. The exception is likely
                # to be very long and gross. Log the full details but just
                # include the summary in the critical error message. TODO:
                # When we expose logs to Stitch users, modify this to
                # suggest looking at the logs for details.
                except RequestException as exc:
                    LOGGER.exception(exc)
                    raise TargetStitchException('Error connecting to Stitch')


class LoggingHandler:  # pylint: disable=too-few-public-methods
    '''Logs records to a local output file.'''
    def __init__(self, output_file, max_batch_bytes, max_batch_records):
        self.output_file = output_file
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_records = max_batch_records

    def handle_batch(self, messages, schema, key_names, bookmark_names=None):
        '''Handles a batch of messages by saving them to a local output file.

        Serializes records in the same way StitchHandler does, so the
        output file should contain the exact request bodies that we would
        send to Stitch.

        '''
        LOGGER.info("Saving batch with %d messages for table %s to %s",
                    len(messages), messages[0].stream, self.output_file.name)
        for i, body in enumerate(serialize(messages,
                                           schema,
                                           key_names,
                                           bookmark_names,
                                           self.max_batch_bytes,
                                           self.max_batch_records)):
            LOGGER.debug("Request body %d is %d bytes", i, len(body))
            self.output_file.write(body)
            self.output_file.write('\n')


class ValidatingHandler: # pylint: disable=too-few-public-methods
    '''Validates input messages against their schema.'''

    def __init__(self):
        getcontext().prec = 76

    def handle_batch(self, messages, schema, key_names, bookmark_names=None): # pylint: disable=no-self-use,unused-argument
        '''Handles messages by validating them against schema.'''
        validator = Draft4Validator(schema, format_checker=FormatChecker())
        for i, message in enumerate(messages):
            if isinstance(message, singer.RecordMessage):
                try:
                    validator.validate(message.record)
                    if key_names:
                        for k in key_names:
                            if k not in message.record:
                                raise TargetStitchException(
                                    'Message {} is missing key property {}'.format(
                                        i, k))
                except Exception as e:
                    raise TargetStitchException(
                        'Record does not pass schema validation: {}'.format(e))

        # pylint: disable=undefined-loop-variable
        # NB: This seems incorrect as there's a chance message is not defined
        LOGGER.info('%s (%s): Batch is valid',
                    message.stream,
                    len(messages))

def generate_sequence(message_num, max_records):
    '''Generates a unique sequence number based on the current time millis
       with a zero-padded message number based on the magnitude of max_records.'''
    sequence_base = str(int(time.time() * SEQUENCE_MULTIPLIER))

    # add an extra order of magnitude to account for the fact that we can
    # actually accept more than the max record count
    fill = len(str(10 * max_records))
    sequence_suffix = str(message_num).zfill(fill)

    return int(sequence_base + sequence_suffix)

def serialize(messages, schema, key_names, bookmark_names, max_bytes, max_records):
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
                'sequence': generate_sequence(idx, max_records)
            }

            if message.time_extracted:
                record_message['time_extracted'] = singer.utils.strftime(message.time_extracted)

            serialized_messages.append(record_message)
        elif isinstance(message, singer.ActivateVersionMessage):
            serialized_messages.append({
                'action': 'activate_version',
                'sequence': generate_sequence(idx, max_records)
            })

    body = {
        'table_name': messages[0].stream,
        'schema': schema,
        'key_names': key_names,
        'messages': serialized_messages
    }
    if messages[0].version is not None:
        body['table_version'] = messages[0].version

    if bookmark_names:
        body['bookmark_names'] = bookmark_names


    # We are not using Decimals for parsing here. We recognize that
    # exposes data to potential rounding errors. However, the Stitch API
    # as it is implemented currently is also subject to rounding errors.
    # This will affect very few data points and we have chosen to leave
    # conversion as is for now.

    serialized = simplejson.dumps(body)
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


class TargetStitch:
    '''Encapsulates most of the logic of target-stitch.

    Useful for unit testing.

    '''

    # pylint: disable=too-many-instance-attributes
    def __init__(self, # pylint: disable=too-many-arguments
                 handlers,
                 state_writer,
                 max_batch_bytes,
                 max_batch_records,
                 batch_delay_seconds):
        self.messages = []
        self.buffer_size_bytes = 0
        self.state = None

        # Mapping from stream name to {'schema': ..., 'key_names': ..., 'bookmark_names': ... }
        self.stream_meta = {}

        # Instance of StitchHandler
        self.handlers = handlers

        # Writer that we write state records to
        self.state_writer = state_writer

        # Batch size limits. Stored as properties here so we can easily
        # change for testing.
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_records = max_batch_records

        # Minimum frequency to send a batch, used with self.time_last_batch_sent
        self.batch_delay_seconds = batch_delay_seconds

        # Time that the last batch was sent
        self.time_last_batch_sent = time.time()



    def flush(self):
        '''Send all the buffered messages to Stitch.'''

        if self.messages:
            stream_meta = self.stream_meta[self.messages[0].stream]
            for handler in self.handlers:
                handler.handle_batch(self.messages,
                                     stream_meta.schema,
                                     stream_meta.key_properties,
                                     stream_meta.bookmark_properties)
            self.time_last_batch_sent = time.time()
            self.messages = []
            self.buffer_size_bytes = 0

        if self.state:
            line = simplejson.dumps(self.state)
            self.state_writer.write("{}\n".format(line))
            self.state_writer.flush()
            self.state = None
            TIMINGS.log_timings()

    def handle_line(self, line):

        '''Takes a raw line from stdin and handles it, updating state and possibly
        flushing the batch to the Gate and the state to the output
        stream.

        '''

        message = singer.parse_message(line)

        # If we got a Schema, set the schema and key properties for this
        # stream. Flush the batch, if there is one, in case the schema is
        # different.
        if isinstance(message, singer.SchemaMessage):
            self.flush()

            self.stream_meta[message.stream] = StreamMeta(
                message.schema,
                message.key_properties,
                message.bookmark_properties)

        elif isinstance(message, (singer.RecordMessage, singer.ActivateVersionMessage)):
            if self.messages and (
                    message.stream != self.messages[0].stream or
                    message.version != self.messages[0].version):
                self.flush()
            self.messages.append(message)
            self.buffer_size_bytes += len(line)

            num_bytes = self.buffer_size_bytes
            num_messages = len(self.messages)
            num_seconds = time.time() - self.time_last_batch_sent

            enough_bytes = num_bytes >= self.max_batch_bytes
            enough_messages = num_messages >= self.max_batch_records
            enough_time = num_seconds >= self.batch_delay_seconds
            if enough_bytes or enough_messages or enough_time:
                LOGGER.debug('Flushing %d bytes, %d messages, after %.2f seconds',
                             num_bytes, num_messages, num_seconds)
                self.flush()

        elif isinstance(message, singer.StateMessage):
            self.state = message.value

            # only check time since state message does not increase num_messages or
            # num_bytes for the batch
            num_seconds = time.time() - self.time_last_batch_sent
            if num_seconds >= self.batch_delay_seconds:
                LOGGER.debug('Flushing %d bytes, %d messages, after %.2f seconds',
                             self.buffer_size_bytes, len(self.messages), num_seconds)
                self.flush()



    def consume(self, reader):
        '''Consume all the lines from the queue, flushing when done.'''
        for line in reader:
            self.handle_line(line)
        self.flush()


def collect():
    '''Send usage info to Stitch.'''

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
        conn.getresponse()
        conn.close()
    except: # pylint: disable=bare-except
        LOGGER.debug('Collection request failed')


def use_batch_url(url):
    '''Replace /import/push with /import/batch in URL'''
    result = url
    if url.endswith('/import/push'):
        result = url.replace('/import/push', '/import/batch')
    LOGGER.info('Using Stitch import URL %s', result)
    return result

def main_impl():
    '''We wrap this function in main() to add exception handling'''
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-c', '--config',
        help='Config file',
        type=argparse.FileType('r'))
    parser.add_argument(
        '-n', '--dry-run',
        help='Dry run - Do not push data to Stitch',
        action='store_true')
    parser.add_argument(
        '-o', '--output-file',
        help='Save requests to this output file',
        type=argparse.FileType('w'))
    parser.add_argument(
        '-v', '--verbose',
        help='Produce debug-level logging',
        action='store_true')
    parser.add_argument(
        '-q', '--quiet',
        help='Suppress info-level logging',
        action='store_true')
    parser.add_argument('--max-batch-records', type=int, default=DEFAULT_MAX_BATCH_RECORDS)
    parser.add_argument('--max-batch-bytes', type=int, default=DEFAULT_MAX_BATCH_BYTES)
    parser.add_argument('--batch-delay-seconds', type=float, default=300.0)
    args = parser.parse_args()

    if args.verbose:
        LOGGER.setLevel('DEBUG')
    elif args.quiet:
        LOGGER.setLevel('WARNING')

    handlers = []
    if args.output_file:
        handlers.append(LoggingHandler(args.output_file,
                                       args.max_batch_bytes,
                                       args.max_batch_records))
    if args.dry_run:
        handlers.append(ValidatingHandler())
    elif not args.config:
        parser.error("config file required if not in dry run mode")
    else:
        config = json.load(args.config)
        token = config.get('token')
        stitch_url = use_batch_url(config.get('stitch_url', DEFAULT_STITCH_URL))

        if not token:
            raise Exception('Configuration is missing required "token" field')

        if not config.get('disable_collection'):
            LOGGER.info('Sending version information to stitchdata.com. ' +
                        'To disable sending anonymous usage data, set ' +
                        'the config parameter "disable_collection" to true')
            Thread(target=collect).start()
        handlers.append(StitchHandler(token,
                                      stitch_url,
                                      args.max_batch_bytes,
                                      args.max_batch_records))

    # queue = Queue(args.max_batch_records)
    reader = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    TargetStitch(handlers,
                 sys.stdout,
                 args.max_batch_bytes,
                 args.max_batch_records,
                 args.batch_delay_seconds).consume(reader)
    LOGGER.info("Exiting normally")

def main():
    '''Main entry point'''
    try:
        MemoryReporter().start()
        main_impl()

    # If we catch an exception at the top level we want to log a CRITICAL
    # line to indicate the reason why we're terminating. Sometimes the
    # extended stack traces can be confusing and this provides a clear way
    # to call out the root cause. If it's a known TargetStitchException we
    # can suppress the stack trace, otherwise we should include the stack
    # trace for debugging purposes, so re-raise the exception.
    except TargetStitchException as exc:
        for line in str(exc).splitlines():
            LOGGER.critical(line)
        sys.exit(1)
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__ == '__main__':
    main()
