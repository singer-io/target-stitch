#!/usr/bin/env python3
# pylint: disable=too-many-arguments,invalid-name,too-many-nested-blocks

'''
Target for Stitch API.
'''

import argparse
import backoff
import io
import json
import pkg_resources
import psutil
import requests
import singer
import sys
import time

from collections import namedtuple
from threading import Thread
from target_stitch.handlers import StitchHandler, LoggingHandler, ValidatingHandler
from target_stitch.exceptions import TargetStitchException

DEFAULT_STITCH_URL = 'https://api.stitchdata.com/v2/import/batch'

MAX_BYTES_PER_FLUSH = 20 * 1000 * 1000

# Cannot be higher than 99999 due to sequence numbers exceeding max long value
MAX_RECORDS_PER_FLUSH = 20000

LOGGER = singer.get_logger().getChild('target_stitch')

# We use this to store schema and key properties from SCHEMA messages
StreamMeta = namedtuple('StreamMeta', ['schema', 'key_properties', 'bookmark_properties'])

def collect():
    '''Send usage info to Stitch.'''
    try:
        version = pkg_resources.get_distribution('target-stitch').version
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-stitch',
            'se_ac': 'open',
            'se_la': version,
        }
        requests.get("https://collector.stitchdata.com/i", params=params)
    except: # pylint: disable=bare-except
        LOGGER.debug('Collection request failed')


# TODO: Is there any reason to not use the DEFAULT_STITCH_URL always?
def use_batch_url(url):
    '''Replace /import/push with /import/batch in URL'''
    result = url
    if url.endswith('/import/push'):
        result = url.replace('/import/push', '/import/batch')
    LOGGER.info('Using Stitch import URL %s', result)
    return result

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



class TargetStitch:
    '''Encapsulates most of the logic of target-stitch.

    Useful for unit testing.
    '''
    # pylint: disable=too-many-instance-attributes
    def __init__(self, # pylint: disable=too-many-arguments
                 handlers,
                 state_writer,
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

        # Minimum frequency to send a batch, used with self.time_last_batch_sent
        self.batch_delay_seconds = batch_delay_seconds

        # Time that the last batch was sent
        self.time_last_batch_sent = time.time()

    def flush(self):
        '''Send all the buffered messages to Stitch.'''
        LOGGER.info('flushing...')
        if self.messages:
            stream_meta = self.stream_meta[self.messages[0].stream]
            for handler in self.handlers:
                handler.handle_batch(self.messages,
                                     self.buffer_size_bytes,
                                     stream_meta.schema,
                                     stream_meta.key_properties,
                                     stream_meta.bookmark_properties)
            self.time_last_batch_sent = time.time()
            self.messages = []
            self.buffer_size_bytes = 0

        if self.state:
            line = json.dumps(self.state)
            self.state_writer.write("{}\n".format(line))
            self.state_writer.flush()
            self.state = None


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

            #don't run out of memory
            if num_bytes >= MAX_BYTES_PER_FLUSH or num_messages >= MAX_RECORDS_PER_FLUSH:
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
    parser.add_argument('--batch-delay-seconds', type=float, default=300.0)
    args = parser.parse_args()

    if args.verbose:
        LOGGER.setLevel('DEBUG')
    elif args.quiet:
        LOGGER.setLevel('WARNING')

    handlers = []
    if args.output_file:
        handlers.append(LoggingHandler(args.output_file))
    if args.dry_run:
        handlers.append(ValidatingHandler())
    elif not args.config:
        parser.error("config file required if not in dry run mode")
    else:
        config = json.load(args.config)
        token = config.get('token')
        client_id = config.get('client_id')
        connection_ns = config.get('connection_ns')
        stitch_url = use_batch_url(config.get('stitch_url', DEFAULT_STITCH_URL))
        spool_host = config.get('stitch_spool_service_host')
        spool_s3_bucket = config.get('stitch_spool_s3_bucket')

        if not token:
            raise Exception('Configuration is missing required "token" field')

        if not config.get('disable_collection'):
            LOGGER.info('Sending version information to stitchdata.com. '
                        'To disable sending anonymous usage data, set '
                        'the config parameter "disable_collection" to true')
            Thread(target=collect).start()
        handlers.append(StitchHandler(token,
                                      client_id,
                                      connection_ns,
                                      stitch_url,
                                      spool_host,
                                      spool_s3_bucket))

    reader = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    TargetStitch(handlers,
                 sys.stdout,
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
