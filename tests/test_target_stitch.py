import unittest
import target_stitch
import json
import io
import mock
import sys
import datetime
import dateutil
import jsonschema
import decimal
from decimal import Decimal
from jsonschema import ValidationError, Draft4Validator, validators, FormatChecker
from strict_rfc3339 import rfc3339_to_timestamp
from dateutil import tz

class DummyClient(target_stitch.GateClient):

    def __init__(self):
        self.batches = []

    def send_batch(self, batch):
        self.batches.append(batch)

def message_lines(messages):
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
        self.target_stitch = target_stitch.TargetStitch(self.client, self.out)

    def test_persist_lines_fails_without_key_properties(self):
        recs = [
            {"type": "SCHEMA",
             "stream": "users",
             "schema": {
                 "properties": {
                     "id": {"type": "integer"},
                     "name": {"type": "string"}}}}]

        with self.assertRaises(Exception):
            for line in message_lines(recs):
                self.target_stitch.handle_line(line)

    def test_persist_lines_works_with_empty_key_properties(self):
        lines = load_sample_lines('empty_key_properties.json')
        with self.target_stitch as target:
            for line in lines:
                target.handle_line(line)
        self.assertEqual(len(self.client.batches), 1)
        self.assertEqual(self.client.batches[0].key_names, [])


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

        with self.target_stitch as target:
            for line in message_lines(inputs):
                target.handle_line(line)
        self.assertEqual(len(self.client.batches), 1)
        batch = self.client.batches[0]
        self.assertEqual(
            batch.schema,
            {
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string"}
                }
            }
        )

        self.assertEqual(batch.key_names, ['id'])


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

        with self.target_stitch as target:
            for line in message_lines(inputs):
                target.handle_line(line)

        expected = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]
        got = [[r['data']['i'] for r in batch.records] for batch in self.client.batches]
        self.assertEqual(got, expected)
        self.assertEqual('1\n4\n9\n', self.out.getvalue())

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

        with self.target_stitch as target:
            for line in message_lines(inputs):
                target.handle_line(line)


        expected = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]
        got = [[r['data']['i'] for r in batch.records] for batch in self.client.batches]
        self.assertEqual(got, expected)
        self.assertEqual('1\n4\n10\n', self.out.getvalue())

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

        with self.target_stitch as target:
            for line in message_lines(inputs):
                target.handle_line(line)

        self.assertEqual(len(self.client.batches), 2)
        self.assertEqual(self.client.batches[0].key_names, ['id'])
        self.assertEqual(self.client.batches[0].schema['properties']['id']['type'], 'integer')
        self.assertEqual(self.client.batches[1].schema['properties']['id']['type'], 'string')

    def test_versioned_stream(self):
        lines = load_sample_lines('versioned_stream.json')
        with self.target_stitch as target:
            for line in lines:
                target.handle_line(line)

        batches = self.client.batches
        self.assertEqual(2, len(batches))
        self.assertEqual(1, batches[0].table_version)
        self.assertEqual(2, batches[1].table_version)
