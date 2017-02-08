import unittest
import target_stitch
import json

class DummyClient(object):

    def __init__(self, callback_function):
        self.callback_function = callback_function
        self.callback_args = []
        self.pending_messages = []
        self.flushed_messages = []
        
    def flush(self):
        self.flushed_messages += self.pending_messages
        self.pending_messages = []
        self.callback_function(self.callback_args)

    def push(self, message, callback_arg=None):
        self.callback_args.append(callback_arg)
        self.pending_messages.append(message)
        if len(self.pending_messages) % 100 == 0:
            self.flush()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.flush()

def persist_all(recs):

    lines = []
    for rec in recs:
        lines.append(json.dumps(rec))
    with DummyClient(lambda x: x) as client:
        target_stitch.persist_lines(client, lines)
        return client.flushed_messages
        
        
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

        
            
        
