import unittest
import target_stitch
import json


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

    def test_persist_lines_emits_state_before_failure(self):
        schema = {"type": "SCHEMA",
                  "stream": "foo",
                  "key_properties": ["i"],
                  "schema": {"properties": {"i": {"type": "integer"}}}
        }
        def state(i):
            return {"type": "STATE", "value": i}
        def record(i):
            return {"type": "RECORD", "stream": "foo", "record": {"i": i}}
        inputs = [
            schema if i == 0 else
            state(i) if i % 2 == 0 else
            record(i) for i in range(10)]
        
        with DummyClient() as client:
            target_stitch.persist_lines(client, message_lines(inputs))
            self.assertEqual(
                [1, 3, 5, 7, 9],
                [m['data']['i'] for m in client.messages])
