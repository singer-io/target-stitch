import json
import unittest

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


class TestValidatingHandler(unittest.TestCase):
    def setUp(self):
        self.handler = validating_handler.ValidatingHandler()

    def test_invalid_schema(self):
        pass

    def test_invalid_record(self):
        pass
