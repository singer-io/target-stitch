#!/usr/bin/env python3
# pylint: disable=too-many-arguments,invalid-name,too-many-nested-blocks,line-too-long,missing-docstring,global-statement, broad-except

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
import functools

from threading import Thread
from contextlib import contextmanager
from collections import namedtuple
from datetime import datetime, timezone
from decimal import Decimal, getcontext
import asyncio
import concurrent
from pprint import pformat
import simplejson
import psutil

import aiohttp
from aiohttp.client_exceptions import ClientConnectorError, ClientResponseError

from jsonschema import ValidationError, Draft4Validator, FormatChecker
import pkg_resources
import backoff

import singer
import ciso8601

LOGGER = singer.get_logger().getChild('target_stitch')

# We use this to store schema and key properties from SCHEMA messages
StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])

BIGBATCH_MAX_BATCH_BYTES = 20000000
DEFAULT_MAX_BATCH_BYTES = 4000000
DEFAULT_MAX_BATCH_RECORDS = 20000
MILLISECOND_SEQUENCE_MULTIPLIER = 1000
NANOSECOND_SEQUENCE_MULTIPLIER = 1000000

# This is our singleton aiohttp session
OUR_SESSION = None

# This datastructure contains our pending aiohttp requests.
# The main thread will read from it.
# The event loop thread will write to it by appending new requests to it and removing completed requests.
PENDING_REQUESTS = []

# This variable holds any exceptions we have encountered sending data to the gate.
# The main thread will read from it and terminate the target if an exception is present.
# The event loop thread will write to it after each aiohttp request completes
SEND_EXCEPTION = None

CONFIG = {}

def start_loop(loop):
    asyncio.set_event_loop(loop)
    global OUR_SESSION
    timeout = aiohttp.ClientTimeout(sock_connect=60, sock_read=60)
    OUR_SESSION = aiohttp.ClientSession(connector=aiohttp.TCPConnector(), timeout=timeout)
    loop.run_forever()

new_loop = asyncio.new_event_loop()
# new_loop.set_debug(True)
t = Thread(target=start_loop, args=(new_loop,))

#The event loop thread should not keep the process alive after the main thread terminates
t.daemon = True

t.start()


class TargetStitchException(Exception):
    '''A known exception for which we don't need to print a stack trace'''

class StitchClientResponseError(Exception):
    def __init__(self, status, response_body):
        self.response_body = response_body
        self.status = status
        super().__init__()

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

def _log_backoff(details):
    (_, exc, _) = sys.exc_info()
    LOGGER.info(
        'Error sending data to Stitch. Sleeping %d seconds before trying again: %s',
        details['wait'], exc)

def parse_config(config_location):
    global CONFIG
    CONFIG = json.load(config_location)
    if not CONFIG.get('token'):
        raise Exception('Configuration is missing required "token" field')

    if not CONFIG.get('client_id'):
        raise Exception('Configuration is missing required "client_id"')

    if not isinstance(CONFIG.get('batch_size_preferences'), dict):
        raise Exception('Configuration is requires batch_size_preferences dictionary')

    if not CONFIG['batch_size_preferences'].get('full_table_streams'):
        CONFIG['batch_size_preferences']['full_table_streams'] = []
    LOGGER.info('Using batch_size_prefernces of %s', CONFIG['batch_size_preferences'])

    if not CONFIG.get('turbo_boost_factor'):
        CONFIG['turbo_boost_factor'] = 1

    if CONFIG['turbo_boost_factor'] != 5:
        LOGGER.info('Using turbo_boost_factor of %s', CONFIG['turbo_boost_factor'])

    if not CONFIG.get('small_batch_url'):
        raise Exception('Configuration is missing required "small_batch_url"')

    if not CONFIG.get('big_batch_url'):
        raise Exception('Configuration is missing required "big_batch_url"')

def determine_stitch_url(stream_name):
    batch_size_prefs = CONFIG.get('batch_size_preferences')
    if stream_name in batch_size_prefs.get('full_table_streams'):
        return CONFIG.get('big_batch_url')

    #eg. platform.heap requires S3 because it is fulltable data
    if batch_size_prefs.get('batch_size_preference') == 'bigbatch':
        return CONFIG.get('big_batch_url')

    if batch_size_prefs.get('batch_size_preference') == 'smallbatch':
        return CONFIG.get('small_batch_url')

    #NB> not implemented yet
    if batch_size_prefs.get('user_batch_size_preference') == 'bigbatch':
        return CONFIG.get('big_batch_url')

    #NB> not implemented yet
    if batch_size_prefs.get('user_batch_size_preference') == 'smallbatch':
        return CONFIG.get('small_batch_url')

    return CONFIG.get('small_batch_url')



class StitchHandler: # pylint: disable=too-few-public-methods
    '''Sends messages to Stitch.'''

    def __init__(self, max_batch_bytes, max_batch_records):
        self.token = CONFIG.get('token')
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_records = max_batch_records

    @staticmethod
    #this happens in the event loop
    def flush_states(state_writer, future):

        global PENDING_REQUESTS
        global SEND_EXCEPTION

        completed_count = 0

        #NB> if/when the first coroutine errors out, we will record it for examination by the main threa.
        #if/when this happens, no further flushing of state should ever occur.  the main thread, in fact,
        #should shutdown quickly after it spots the exception
        if SEND_EXCEPTION is None:
            SEND_EXCEPTION = future.exception()

        if SEND_EXCEPTION is not None:
            LOGGER.info('FLUSH early exit because of SEND_EXCEPTION: %s', pformat(SEND_EXCEPTION))
            return

        try:
            for f, s in PENDING_REQUESTS:
                if f.done():
                    completed_count = completed_count + 1
                    #NB> this is a very import line.
                    #NEVER blinding emit state just because a coroutine has completed.
                    #if this were None, we would have just nuked the client's state
                    if s:
                        line = simplejson.dumps(s)
                        state_writer.write("{}\n".format(line))
                        state_writer.flush()
                else:
                    break

            PENDING_REQUESTS = PENDING_REQUESTS[completed_count:]

        except BaseException as err:
            SEND_EXCEPTION = err


    def headers(self):
        '''Return the headers based on the token'''
        return {
            'Authorization': 'Bearer {}'.format(self.token),
            'Content-Type': 'application/json'
        }

    def send(self, data, contains_activate_version, state_writer, state, stitch_url):
        '''Send the given data to Stitch, retrying on exceptions'''
        global PENDING_REQUESTS
        global SEND_EXCEPTION

        check_send_exception()

        headers = self.headers()
        verify_ssl = True
        if os.environ.get("TARGET_STITCH_SSL_VERIFY") == 'false':
            verify_ssl = False

        LOGGER.info("Sending batch of %d bytes to %s", len(data), stitch_url)

        #NB> before we send any activate_versions we must ensure that all PENDING_REQUETS complete.
        #this is to ensure ordering in the case of Full Table replication where the Activate Version,
        #must arrive AFTER all of the relevant data.
        if len(PENDING_REQUESTS) > 0 and contains_activate_version:
            LOGGER.info('Sending batch with ActivateVersion. Flushing PENDING_REQUESTS first')
            finish_requests()

        if len(PENDING_REQUESTS) >= CONFIG.get('turbo_boost_factor'):

            #wait for to finish the first future before resuming the main thread
            finish_requests(CONFIG.get('turbo_boost_factor') - 1)

        #NB> this schedules the task on the event loop thread.
        #    it will be executed at some point in the future
        future = asyncio.run_coroutine_threadsafe(post_coroutine(stitch_url, headers, data, verify_ssl), new_loop)
        next_pending_request = (future, state)
        PENDING_REQUESTS.append(next_pending_request)
        future.add_done_callback(functools.partial(self.flush_states, state_writer))


    def handle_state_only(self, state_writer=None, state=None):
        async def fake_future_fn():
            pass

        global PENDING_REQUESTS
        #NB> no point in sending out this state if a previous request has failed
        check_send_exception()
        future = asyncio.run_coroutine_threadsafe(fake_future_fn(), new_loop)
        next_pending_request = (future, state)
        PENDING_REQUESTS.append(next_pending_request)

        future.add_done_callback(functools.partial(self.flush_states, state_writer))


    def handle_batch(self, messages, contains_activate_version, schema, key_names, bookmark_names=None, state_writer=None, state=None, ):
        '''Handle messages by sending them to Stitch.

        If the serialized form of the messages is too large to fit into a
        single request this will break them up into multiple smaller
        requests.

        '''

        stitch_url = determine_stitch_url(messages[0].stream)
        LOGGER.info("Serializing batch with %d messages for table %s", len(messages), messages[0].stream)
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
                if len(body) > DEFAULT_MAX_BATCH_BYTES:
                    stitch_url = CONFIG.get('big_batch_url')

                flushable_state = None
                if i + 1 == len(bodies):
                    flushable_state = state

                self.send(body, contains_activate_version, state_writer, flushable_state, stitch_url)


class LoggingHandler:  # pylint: disable=too-few-public-methods
    '''Logs records to a local output file.'''
    def __init__(self, output_file, max_batch_bytes, max_batch_records):
        self.output_file = output_file
        self.max_batch_bytes = max_batch_bytes
        self.max_batch_records = max_batch_records

    # pylint: disable=R0201
    def handle_state_only(self, state_writer=None, state=None):
        LOGGER.info("LoggingHandler handle_state_only: %s", state)
        if state:
            line = simplejson.dumps(state)
            state_writer.write("{}\n".format(line))
            state_writer.flush()


    def handle_batch(self, messages, contains_activate_version, schema, key_names, bookmark_names=None, state_writer=None, state=None): #pylint: disable=unused-argument
        '''Handles a batch of messages by saving them to a local output file.

        Serializes records in the same way StitchHandler does, so the
        output file should contain the exact request bodies that we would
        send to Stitch.

        '''
        LOGGER.info("LoggingHandler handle_batch")
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

        if state:
            line = simplejson.dumps(state)
            state_writer.write("{}\n".format(line))
            state_writer.flush()



class ValidatingHandler: # pylint: disable=too-few-public-methods
    '''Validates input messages against their schema.'''

    def __init__(self):
        getcontext().prec = 76

    # pylint: disable=R0201
    def handle_state_only(self, state_writer=None, state=None):
        LOGGER.info("ValidatingHandler handle_state_only: %s", state)
        if state:
            line = simplejson.dumps(state)
            state_writer.write("{}\n".format(line))
            state_writer.flush()

    def handle_batch(self, messages, contains_activate_version, schema, key_names, bookmark_names=None, state_writer=None, state=None): # pylint: disable=no-self-use,unused-argument
        '''Handles messages by validating them against schema.'''
        LOGGER.info("ValidatingHandler handle_batch")
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
                    messages[0].stream,
                    len(messages))
        if state:
            line = simplejson.dumps(state)
            state_writer.write("{}\n".format(line))
            state_writer.flush()

def generate_sequence(message_num, max_records):
    '''
    Generates a unique sequence number based on the current time in nanoseconds
    with a zero-padded message number based on the index of the record within the
    magnitude of max_records.

    COMPATIBILITY:
    Maintains a historical width of 19 characters (with default `max_records`), in order
    to not overflow downstream processes that depend on the width of this number.

    Because of this requirement, `message_num` is modulo the difference between nanos
    and millis to maintain 19 characters.
    '''
    nanosecond_sequence_base = str(int(time.time() * NANOSECOND_SEQUENCE_MULTIPLIER))
    modulo = NANOSECOND_SEQUENCE_MULTIPLIER / MILLISECOND_SEQUENCE_MULTIPLIER
    zfill_width_mod = len(str(NANOSECOND_SEQUENCE_MULTIPLIER)) - len(str(MILLISECOND_SEQUENCE_MULTIPLIER))

    # add an extra order of magnitude to account for the fact that we can
    # actually accept more than the max record count
    fill = len(str(10 * max_records)) - zfill_width_mod
    sequence_suffix = str(int(message_num % modulo)).zfill(fill)

    return int(nanosecond_sequence_base + sequence_suffix)

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
                #"%04Y-%m-%dT%H:%M:%S.%fZ"
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
        if len(serialized) < BIGBATCH_MAX_BATCH_BYTES:
            return [serialized]
        raise BatchTooLargeException(
            "A single record is larger than the Stitch API limit of {} Mb".format(
                BIGBATCH_MAX_BATCH_BYTES // 1000000))


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
        self.contains_activate_version = False
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
                                     self.contains_activate_version,
                                     stream_meta.schema,
                                     stream_meta.key_properties,
                                     stream_meta.bookmark_properties,
                                     self.state_writer,
                                     self.state)

            self.time_last_batch_sent = time.time()
            self.messages = []
            self.contains_activate_version = False
            self.state = None
            self.buffer_size_bytes = 0

        # NB> State is usually handled above but in the case there are no messages
        # we still want to ensure state is emitted.
        elif self.state:
            for handler in self.handlers:
                handler.handle_state_only(self.state_writer, self.state)
            self.state = None
            TIMINGS.log_timings()


    def handle_line(self, line):

        '''Takes a raw line from stdin and handles it, updating state and possibly
        flushing the batch to the Gate and the state to the output
        stream.

        '''

        message = overloaded_parse_message(line)

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
            if isinstance(message, singer.ActivateVersionMessage):
                self.contains_activate_version = True

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
                self.time_last_batch_sent = time.time()



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
        parse_config(args.config)

        if not CONFIG.get('disable_collection'):
            LOGGER.info('Sending version information to stitchdata.com. ' +
                        'To disable sending anonymous usage data, set ' +
                        'the config parameter "disable_collection" to true')
            Thread(target=collect).start()
        handlers.append(StitchHandler(args.max_batch_bytes,
                                      args.max_batch_records))

    # queue = Queue(args.max_batch_records)
    reader = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    target_stitch = TargetStitch(handlers,
                                 sys.stdout,
                                 args.max_batch_bytes,
                                 args.max_batch_records,
                                 args.batch_delay_seconds)
    target_stitch.consume(reader)

    #NB> we need to wait for this to be empty indicating that all of the
    #requests have been finished and their states flushed
    finish_requests()
    LOGGER.info("Requests complete, stopping loop")
    new_loop.call_soon_threadsafe(new_loop.stop)


def finish_requests(max_count=0):
    global PENDING_REQUESTS
    while True:
        # LOGGER.info("Finishing %s requests:", len(PENDING_REQUESTS))
        check_send_exception()
        if len(PENDING_REQUESTS) <= max_count: #pylint: disable=len-as-condition
            break
        time.sleep(1 / 1000.0)



def check_send_exception():
    try:
        global SEND_EXCEPTION
        if SEND_EXCEPTION:
            raise SEND_EXCEPTION

    # An StitchClientResponseError means we received > 2xx response
    # Try to parse the "message" from the
    # json body of the response, since Stitch should include
    # the human-oriented message in that field. If there are
    # any errors parsing the message, just include the
    # stringified response.
    except StitchClientResponseError as exc:
        try:
            msg = "{}: {}".format(str(exc.status), exc.response_body)
        except: # pylint: disable=bare-except
            LOGGER.exception('Exception while processing error response')
            msg = '{}'.format(exc)
        raise TargetStitchException('Error persisting data to Stitch: ' +
                                    msg)

    # A ClientConnectorErrormeans we
    # couldn't even connect to stitch. The exception is likely
    # to be very long and gross. Log the full details but just
    # include the summary in the critical error message.
    except ClientConnectorError as exc:
        LOGGER.exception(exc)
        raise TargetStitchException('Error connecting to Stitch')

    except concurrent.futures._base.TimeoutError as exc: #pylint: disable=protected-access
        raise TargetStitchException("Timeout sending to Stitch")


def exception_is_4xx(ex):
    return 400 <= ex.status < 500

@backoff.on_exception(backoff.expo,
                      StitchClientResponseError,
                      max_tries=5,
                      giveup=exception_is_4xx,
                      on_backoff=_log_backoff)
async def post_coroutine(url, headers, data, verify_ssl):
    # LOGGER.info("POST starting: %s ssl(%s)", url, verify_ssl)
    global OUR_SESSION
    async with OUR_SESSION.post(url, headers=headers, data=data, raise_for_status=False, verify_ssl=verify_ssl) as response:
        result_body = None
        try:
            result_body = await response.json()
        except BaseException as ex: #pylint: disable=unused-variable
            raise StitchClientResponseError(response.status, "unable to parse response body as json")

        if response.status // 100 != 2:
            raise StitchClientResponseError(response.status, result_body)

        return result_body

def _required_key(msg, k):
    if k not in msg:
        raise Exception("Message is missing required key '{}': {}".format(k, msg))

    return msg[k]

def overloaded_parse_message(msg):
    """Parse a message string into a Message object."""

    # We are not using Decimals for parsing here.
    # We recognize that exposes data to potentially
    # lossy conversions.  However, this will affect
    # very few data points and we have chosen to
    # leave conversion as is for now.
    obj = simplejson.loads(msg, use_decimal=True)
    msg_type = _required_key(obj, 'type')

    if msg_type == 'RECORD':
        time_extracted = obj.get('time_extracted')
        if time_extracted:
            try:
                time_extracted = ciso8601.parse_datetime(time_extracted)
            except Exception:
                time_extracted = None
        return singer.RecordMessage(stream=_required_key(obj, 'stream'),
                                    record=_required_key(obj, 'record'),
                                    version=obj.get('version'),
                                    time_extracted=time_extracted)

    if msg_type == 'SCHEMA':
        return singer.SchemaMessage(stream=_required_key(obj, 'stream'),
                                    schema=_required_key(obj, 'schema'),
                                    key_properties=_required_key(obj, 'key_properties'),
                                    bookmark_properties=obj.get('bookmark_properties'))

    if msg_type == 'STATE':
        return singer.StateMessage(value=_required_key(obj, 'value'))

    if msg_type == 'ACTIVATE_VERSION':
        return singer.ActivateVersionMessage(stream=_required_key(obj, 'stream'),
                                             version=_required_key(obj, 'version'))
    return None


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
