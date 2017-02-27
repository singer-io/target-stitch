import unittest
import target_stitch
import json


class DummyClient(target_stitch.DryRunClient):

    def __init__(self):
        super().__init__()
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
        with self.assertRaises(Exception):
            messages = persist_all([
                {"type": "SCHEMA",
                 "stream": "users",
                 "schema": {
                     "properties": {
                         "id": {"type": "integer"},
                         "name": {"type": "string"}}}}])

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
        
        outputs = persist_all(inputs)
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0]['key_names'], ['id'])

    def test_persist_lines_emits_state_before_failure(self):
        lines = '''
{"type": "RECORD", "record": {"name": "foo"}}
{"type": "RECORD", "record": {"name": "bar"}}
{"type": "STATE", "value": {"seq": 1}}
boom
'''
        
