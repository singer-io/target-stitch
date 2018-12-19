import json
import unittest
import singer

from target_stitch.exceptions import TargetStitchException
from target_stitch.handlers import validating_handler


def mk_schema(stream, schema):
    return json.dumps({
        "type": "SCHEMA",
        "stream": stream,
        "key_properties": ["name"],
        "schema": {
            "type": "object",
            "properties": schema,
        },
    })


def mk_record(stream, record, version=None):
    msg = {
        "type": "RECORD",
        "stream": stream,
        "record": record,
    }
    if version:
        msg["version"] = version

    return json.dumps(msg)


class TestValidatingHandler(unittest.TestCase):
    def setUp(self):
        self.handler = validating_handler.ValidatingHandler()

    def test_valid_record(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data)
        messages = [singer.parse_message(record)]
        self.handler.handle_batch(messages,
                                  1,
                                  schema.schema,
                                  schema.key_properties,
                                  schema.bookmark_properties)

    def test_invalid_schema(self):
        schema_data = {"name": {"type": "orange"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data)
        messages = [singer.parse_message(record)]
        with self.assertRaises(TargetStitchException):
            self.handler.handle_batch(messages,
                                      1,
                                      schema.schema,
                                      schema.key_properties,
                                      schema.bookmark_properties)

    def test_invalid_record(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": 0}
        record = mk_record("test1", record_data)
        messages = [singer.parse_message(record)]
        with self.assertRaises(TargetStitchException):
            self.handler.handle_batch(messages,
                                      1,
                                      schema.schema,
                                      schema.key_properties,
                                      schema.bookmark_properties)

    def test_missing_key_property(self):
        schema_data = {"name": {"type": ["null", "string"]}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {}
        record = mk_record("test1", record_data)
        messages = [singer.parse_message(record)]
        with self.assertRaises(TargetStitchException):
            self.handler.handle_batch(messages,
                                      1,
                                      schema.schema,
                                      schema.key_properties,
                                      schema.bookmark_properties)
