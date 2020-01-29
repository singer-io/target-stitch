import unittest
import target_stitch
import json
import io
import mock
import sys
import datetime
import pytz
import jsonschema
import simplejson
import decimal
import re

from decimal import Decimal
from jsonschema import ValidationError, Draft4Validator, validators, FormatChecker
from singer import ActivateVersionMessage, RecordMessage, utils, parse_message


class DummyClient(object):

    def __init__(self):
        self.batches = []

    def handle_batch(self, messages, contains_activate_version, schema, key_names, bookmark_names, state_writer, state):
        self.batches.append(
            {'messages': messages,
             'schema': schema,
             'key_names': key_names,
             'bookmark_names': bookmark_names})

def message_queue(messages):
    return [json.dumps(m) for m in messages]

def persist_all(recs):
    with DummyClient() as client:
        target_stitch.persist_lines(client, message_lines(recs))
        return client.messages


def state(i):
    return {"type": "STATE", "value": i}
def record(i):
    return {"type": "RECORD", "stream": "foo", "record": {"i": i}}

schema = {"type": "SCHEMA",
          "stream": "foo",
          "key_properties": ["i"],
          "schema": {"properties": {"i": {"type": "integer"}}}
}

def load_sample_lines(filename):
    with open('tests/' + filename) as fp:
        return [line for line in fp]


class TestTargetStitch(unittest.TestCase):

    def setUp(self):
        self.client = DummyClient()
        self.out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [self.client], self.out, 4000000, 20000, 100000)

    def test_persist_lines_fails_without_key_properties(self):
        recs = [
            {"type": "SCHEMA",
             "stream": "users",
             "schema": {
                 "properties": {
                     "id": {"type": "integer"},
                     "name": {"type": "string"}}}}]

        with self.assertRaises(Exception):
            target_stitch.consume(message_queue(recs))

    def test_persist_lines_works_with_empty_key_properties(self):
        queue = load_sample_lines('empty_key_properties.json')
        self.target_stitch.consume(queue)
        self.assertEqual(len(self.client.batches), 1)
        self.assertEqual(self.client.batches[0]['key_names'], [])


    def test_persist_lines_sets_key_names(self):
        inputs = [
            {"type": "SCHEMA",
             "stream": "users",
             "key_properties": ["id"],
             "schema": {
                 "properties": {
                     "id": {"type": "integer"},
                     "name": {"type": "string"}}}},
            {"type": "RECORD",
             "stream": "users",
             "record": {"id": 1, "name": "mike"}}]

        self.target_stitch.consume(message_queue(inputs))
        self.assertEqual(len(self.client.batches), 1)
        batch = self.client.batches[0]
        self.assertEqual(
            batch['schema'],
            {
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"}
                }
            }
        )

        self.assertEqual(batch['key_names'], ['id'])

    def test_persist_last_state_when_stream_ends_with_record(self):
        self.target_stitch.max_batch_records = 3
        inputs = [
            schema,
            record(0), state(0), record(1), state(1), record(2),
            # flush state 1
            state(2), record(3), state(3), record(4), state(4), record(5),
            # flush state 4
            record(6),
            record(7),
            record(8),
            # flush empty states
            state(8),
            record(9),
            state(9),
            record(10)]

        self.target_stitch.consume(message_queue(inputs))

        expected = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]
        got = [[r.record['i'] for r in batch['messages']] for batch in self.client.batches]
        self.assertEqual(got, expected)

    def test_persist_last_state_when_stream_ends_with_state(self):
        self.target_stitch.max_batch_records = 3
        inputs = [
            schema,
            record(0), state(0), record(1), state(1), record(2),
            # flush state 1
            state(2), record(3), state(3), record(4), state(4), record(5),
            # flush state 4
            record(6),
            record(7),
            record(8),
            # flush empty states
            state(8),
            record(9),
            state(9),
            record(10),
            state(10)]

        self.target_stitch.consume(message_queue(inputs))


        expected = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]
        got = [[r.record['i'] for r in batch['messages']] for batch in self.client.batches]
        self.assertEqual(got, expected)

    def test_time_triggered_persist(self):
        self.target_stitch.batch_delay_seconds = -1
        self.target_stitch.max_batch_records = 10000
        inputs = [
            schema,
            record(0),
            record(1),
            record(2)]
        self.target_stitch.consume(message_queue(inputs))
        expected = [[0], [1], [2]]
        got = [[r.record['i'] for r in batch['messages']] for batch in self.client.batches]
        self.assertEqual(got, expected)

    def test_persist_lines_updates_schema(self):
        inputs = [
            {"type": "SCHEMA",
             "stream": "users",
             "key_properties": ["id"],
             "schema": {
                 "properties": {
                     "id": {"type": "integer"},
                     "name": {"type": "string"}}}},
            {"type": "RECORD",
             "stream": "users",
             "record": {"id": 1, "name": "mike"}},
            {"type": "SCHEMA",
             "stream": "users",
             "key_properties": ["id"],
             "schema": {
                 "properties": {
                     "id": {"type": "string"},
                     "name": {"type": "string"}}}},
            {"type": "RECORD",
             "stream": "users",
             "record": {"id": "1", "name": "mike"}}]

        self.target_stitch.consume(message_queue(inputs))

        self.assertEqual(len(self.client.batches), 2)
        self.assertEqual(self.client.batches[0]['key_names'], ['id'])
        self.assertEqual(self.client.batches[0]['schema']['properties']['id']['type'], 'integer')
        self.assertEqual(self.client.batches[1]['schema']['properties']['id']['type'], 'string')

    def test_versioned_stream(self):
        queue = load_sample_lines('versioned_stream.json')
        self.target_stitch.consume(queue)

        batches = self.client.batches
        self.assertEqual(2, len(batches))
        self.assertEqual(1, batches[0]['messages'][0].version)
        self.assertEqual(2, batches[1]['messages'][0].version)

EXPECTED_BATCH_TOO_LARGE_MESSAGE = """\
A single record is larger than the Stitch API limit of 20 Mb
The 5 largest fields are:
.*a.*\d+ bytes
.*b.*\d+ bytes"""

class TestSerialize(unittest.TestCase):

    def setUp(self):
        self.schema = {
            'type': 'object',
            'properties': {
                'id': {'type': 'integer'},
                'color': {'type': 'string'}
            }
        }

        self.colors = ['red', 'orange', 'yellow', 'green', 'blue', 'indigo', 'violet']
        self.key_names = ['id']
        self.bookmark_names = ['updated_at']

        self.records = [{'id': i, 'color': color, 'updated_at': utils.strftime(utils.now())}
                        for i, color in enumerate(self.colors)]
        self.messages = [RecordMessage(stream='colors', record=r) for r in self.records]
        self.messages.append(ActivateVersionMessage(stream='colors', version=1))

    def serialize_with_limit(self, limit):
        return target_stitch.serialize(self.messages, self.schema, self.key_names, self.bookmark_names, limit, target_stitch.DEFAULT_MAX_BATCH_RECORDS)

    def unpack_colors(self, request_bodies):
        colors = []
        for body in request_bodies:
            loaded = json.loads(body)
            for message in loaded['messages']:
                action = message['action']
                if action == 'upsert':
                    colors.append((action, message['data']['color']))
                else:
                    colors.append((action))
        return colors

    def test_splits_batches(self):
        self.assertEqual(1, len(self.serialize_with_limit(2000)))
        self.assertEqual(2, len(self.serialize_with_limit(1000)))
        self.assertEqual(4, len(self.serialize_with_limit(500)))
        self.assertEqual(8, len(self.serialize_with_limit(385)))

    def test_raises_if_cant_stay_in_limit(self):
        data = 'a' * 21000000
        data = json.dumps({"a": "a" * 20000000, "b": "b" * 10000000})
        message = RecordMessage(stream='colors', record=data)
        with self.assertRaisesRegex(target_stitch.BatchTooLargeException, re.compile(EXPECTED_BATCH_TOO_LARGE_MESSAGE)):
            target_stitch.serialize([message], self.schema, self.key_names, self.bookmark_names, 4000000, target_stitch.DEFAULT_MAX_BATCH_RECORDS)

    def test_does_not_drop_records(self):
        expected = [
            ('upsert', 'red'),
            ('upsert', 'orange'),
            ('upsert', 'yellow'),
            ('upsert', 'green'),
            ('upsert', 'blue'),
            ('upsert', 'indigo'),
            ('upsert', 'violet'),
            ('activate_version')]

        self.assertEqual(expected, self.unpack_colors(self.serialize_with_limit(2000)))
        self.assertEqual(expected, self.unpack_colors(self.serialize_with_limit(1000)))
        self.assertEqual(expected, self.unpack_colors(self.serialize_with_limit(500)))
        self.assertEqual(expected, self.unpack_colors(self.serialize_with_limit(385)))

    def test_serialize_time_extracted(self):
        """ Test that we're not corrupting timestamps with cross platform parsing. (Test case for OSX, specifically) """
        expected = "1970-01-01T03:45:23.000000Z"
        test_time = datetime.datetime(1970, 1, 1, 3, 45, 23, tzinfo=pytz.utc)

        record = [RecordMessage("greetings",'{greeting: "hi"}', time_extracted=test_time)]
        schema = '{"type": "object", "properties": {"greeting": {"type": "string"}}}'
        batch = target_stitch.serialize(record, schema, [], [], 1000, target_stitch.DEFAULT_MAX_BATCH_RECORDS)[0]
        actual = json.loads(batch)["messages"][0]["time_extracted"]

        self.assertEqual(expected, actual)


    def create_raw_record(self, value):
        return '{"value": ' + value + '}'

    def create_raw_record_message(self,raw_record):
        return '{"type": "RECORD", "stream": "test", "record": ' + raw_record + '}'

class TestDetermineStitchUrl(unittest.TestCase):
    def test_full_table_stream(self):
        big_batch_url = 'https://bigbatches.org'
        small_batch_url = 'https://smallbatch.mil'
        target_stitch.CONFIG = {'batch_size_preferences' :
                                {'full_table_streams' : ['chickens'],
                                 'batch_size_preference' : None,
                                 'user_batch_size_preference' : None
                                },
                                'small_batch_url' : small_batch_url,
                                'big_batch_url' : big_batch_url}

        self.assertEqual(target_stitch.determine_stitch_url('chickens'), big_batch_url)

    def test_incremental_stream(self):
        big_batch_url = 'https://bigbatches.org'
        small_batch_url = 'https://smallbatch.mil'
        target_stitch.CONFIG = {'batch_size_preferences' :
                                {'full_table_streams' : [],
                                 'batch_size_preference' : None,
                                 'user_batch_size_preference' : None
                                },
                                'small_batch_url' : small_batch_url,
                                'big_batch_url' : big_batch_url}

        self.assertEqual(target_stitch.determine_stitch_url('chickens'), small_batch_url)

    def test_big_batch_preference(self):
        big_batch_url = 'https://bigbatches.org'
        small_batch_url = 'https://smallbatch.mil'
        target_stitch.CONFIG = {'batch_size_preferences' :
                                {'full_table_streams' : [],
                                 'batch_size_preference' : 'bigbatch',
                                 'user_batch_size_preference' : None
                                },
                                'small_batch_url' : small_batch_url,
                                'big_batch_url' : big_batch_url}

        self.assertEqual(target_stitch.determine_stitch_url('chickens'), big_batch_url)


if __name__== "__main__":
    test1 = TestSerialize()
    test1.setUp()
    test1.test_raises_if_cant_stay_in_limit()
