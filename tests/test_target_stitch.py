import unittest
import target_stitch
import json
import io
import mock
import sys
import datetime
import jsonschema

class DummyClient(target_stitch.DryRunClient):

    def __init__(self, buffer_size=100):
        super().__init__(buffer_size=buffer_size)
        self.messages = []

    def push(self, message, callback_arg=None):
        self.messages.append(message)
        super().push(message, callback_arg)


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


class TestTargetStitch(unittest.TestCase):

    def test_persist_lines_fails_without_key_properties(self):
        recs = [
            {"type": "SCHEMA",
             "stream": "users",
             "schema": {
                 "properties": {
                     "id": {"type": "integer"},
                     "name": {"type": "string"}}}}]

        with DummyClient() as client:
            with self.assertRaises(Exception):
                target_stitch.persist_lines(client, message_lines(recs))

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

        with DummyClient() as client:
            target_stitch.persist_lines(client, message_lines(inputs))
            self.assertEqual(len(client.messages), 1)
            self.assertEqual(client.messages[0]['key_names'], ['id'])

    def test_persist_lines_converts_date_time(self):
        inputs = [
            {"type": "SCHEMA",
             "stream": "users",
             "key_properties": ["id"],
             "schema": {
                 "properties": {
                     "id": {"type": "integer"},
                     "t": {"type": "string", "format": "date-time"}}}},
            {"type": "RECORD",
             "stream": "users",
             "record": {"id": 1, "t": "2017-02-27T00:00:00+00:00"}}]

        with DummyClient() as client:
            target_stitch.persist_lines(client, message_lines(inputs))
            dt = client.messages[0]['data']['t']
            self.assertEqual(datetime.datetime, type(dt))

    def test_persist_lines_fails_if_doesnt_fit_schema(self):
        inputs = [
            {"type": "SCHEMA",
             "stream": "users",
             "key_properties": ["id"],
             "schema": {
                 "properties": {
                     "id": {"type": "integer"},
                     "t": {"type": "string", "format": "date-time"}}}},
            {"type": "RECORD",
             "stream": "users",
             "record": {"id": 1, "t": "foobar"}}]


        with DummyClient() as client:
            with self.assertRaises(jsonschema.exceptions.ValidationError):
                target_stitch.persist_lines(client, message_lines(inputs))


    def test_persist_last_state_when_stream_ends_with_record(self):

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

        buf = io.StringIO()
        with mock.patch('sys.stdout', buf):
            with DummyClient(buffer_size=3) as client:

                final_state = target_stitch.persist_lines(client, message_lines(inputs))

                self.assertEqual(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    [m['data']['i'] for m in client.messages])
                self.assertIsNone(final_state)
            self.assertEqual('1\n4\n9\n', sys.stdout.getvalue())

    def test_persist_last_state_when_stream_ends_with_state(self):

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

        buf = io.StringIO()
        with mock.patch('sys.stdout', buf):
            with DummyClient(buffer_size=3) as client:

                final_state = target_stitch.persist_lines(client, message_lines(inputs))

                self.assertEqual(
                    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                    [m['data']['i'] for m in client.messages])
                self.assertEqual(10, final_state)
            self.assertEqual('1\n4\n9\n', sys.stdout.getvalue())
