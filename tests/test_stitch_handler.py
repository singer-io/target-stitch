import io
import json
import unittest
from unittest import mock
from transit.reader import Reader
import datetime
import pytz
from decimal import Decimal

import singer
from target_stitch.handlers import stitch_handler


test_client_id = 1
test_namespace = "test"

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


def mk_version(stream, version):
    return json.dumps({
        "type": "ACTIVATE_VERSION",
        "stream": stream,
        "version": version,
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


def decode_transit(data):
    buf = io.BytesIO(data)
    reader = Reader("msgpack")
    return dict(reader.read(buf))


class TestStitchHandler(unittest.TestCase):
    def setUp(self):
        with mock.patch('boto.connect_s3'):
            self.handler = stitch_handler.StitchHandler(
                token="token",
                client_id=test_client_id,
                connection_ns=test_namespace,
                stitch_url="gate",
                spool_host="spool",
                spool_s3_bucket="bucket",
            )

    def test_post_to_gate(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data)
        messages = [singer.parse_message(record)]
        with mock.patch.object(self.handler, 'send') as mock_post:
            self.handler.handle_batch(
                messages,
                1,
                schema.schema,
                schema.key_properties,
                schema.bookmark_properties)

        actual = json.loads(mock_post.call_args[0][0])
        self.assertEqual("test1", actual["table_name"])
        self.assertEqual(schema.schema, actual["schema"])
        self.assertEqual(schema.key_properties, actual["key_names"])
        self.assertEqual(1, len(actual["messages"]))
        self.assertEqual(record_data, actual["messages"][0]["data"])
        self.assertEqual("upsert", actual["messages"][0]["action"])
        self.assertIn("sequence", actual["messages"][0])

    def test_post_to_s3(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"
        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                self.handler.handle_batch(
                    messages,
                    stitch_handler.S3_THRESHOLD_BYTES + 1,
                    schema.schema,
                    schema.key_properties,
                    schema.bookmark_properties)
        
        self.assertEqual(2, len(mock_post.call_args_list))

        actual = mock_post.call_args_list[0][0][0]
        expected = {
            "table_name": "test1",
            "table_version": 1,
            "action": "upsert",
            "s3_bucket": "bucket",
            "s3_key": key_name,
            "persist_duration_millis": 1,
            "format": "transit+msgpack",
            "bookmark_metadata": None,
        }
        self.assertDictContainsSubset(expected, actual)

        actual = mock_post.call_args_list[1][0][0]
        expected = {
            "table_name": "test1",
            "table_version": 1,
            "action": "switch_view",
            "s3_bucket": "bucket",
            "s3_key": key_name,
            "persist_duration_millis": 1,
            "format": "transit+msgpack",
            "bookmark_metadata": None,
        }
        self.assertDictContainsSubset(expected, actual)

        actual = decode_transit(mock_post_s3.call_args_list[0][0][0])
        self.assertEqual(2, actual["message_version"])
        self.assertEqual("2", actual["pipeline_version"])

        expected = {
            "namespace": "test",
            "table_name": "test1",
            "action": "upsert",
            "client_id": test_client_id,
        }
        self.assertDictContainsSubset(expected, dict(actual["body"]))
        self.assertIn("sequence", actual["body"])
        self.assertIn("key_names", actual["body"])
        self.assertDictEqual(record_data, dict(actual["body"]["data"]))

    def test_same_path(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"

        with mock.patch.object(self.handler, 'send') as mock_post_gate:
            with mock.patch.object(self.handler, 'post_to_spool') as mock_post_spool:
                with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                    self.handler.handle_batch(
                        messages,
                        stitch_handler.S3_THRESHOLD_BYTES + 1,
                        schema.schema,
                        schema.key_properties,
                        schema.bookmark_properties)

                    self.handler.handle_batch(
                        messages,
                        1,
                        schema.schema,
                        schema.key_properties,
                        schema.bookmark_properties)

        mock_post_gate.assert_not_called()
        self.assertEqual(4, len(mock_post_spool.call_args_list))
        self.assertEqual(4, len(mock_post_s3.call_args_list))

    def test_invalid_schema(self):
        schema_data = {"name": {"type": "orange"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": "test1-0"}
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"

        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                with self.assertRaises(ValueError):
                    self.handler.handle_batch(
                        messages,
                        stitch_handler.S3_THRESHOLD_BYTES + 1,
                        schema.schema,
                        schema.key_properties,
                        schema.bookmark_properties)

    def test_invalid_record(self):
        schema_data = {"name": {"type": "string"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"name": 1}
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"
        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                with self.assertRaises(ValueError):
                    self.handler.handle_batch(
                        messages,
                        stitch_handler.S3_THRESHOLD_BYTES + 1,
                        schema.schema,
                        schema.key_properties,
                        schema.bookmark_properties)

    def test_datetimes(self):
        schema_data = {"date": {"type": "string", "format": "date-time"}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"date": "2018-01-01T00:00:00Z"}
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"
        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                self.handler.handle_batch(
                    messages,
                    stitch_handler.S3_THRESHOLD_BYTES + 1,
                    schema.schema,
                    schema.key_properties,
                    schema.bookmark_properties)

        actual = decode_transit(mock_post_s3.call_args_list[0][0][0])
        expected = {
            "date": datetime.datetime(2018, 1, 1, tzinfo=pytz.UTC),
        }
        self.assertDictEqual(expected, dict(actual["body"]["data"]))

    def test_decimals(self):
        schema_data = {"money": {"type": "number", "multipleOf": 0.01}}
        schema = singer.parse_message(mk_schema("test1", schema_data))
        record_data = {"money": 10.42}
        record = mk_record("test1", record_data, 1)
        version = mk_version("test1", 1)
        messages = [singer.parse_message(record), singer.parse_message(version)]
        key_name = "test-key"

        with mock.patch.object(self.handler, 'post_to_spool') as mock_post:
            with mock.patch.object(self.handler, 'post_to_s3', return_value=[key_name, 1]) as mock_post_s3:
                self.handler.handle_batch(
                    messages,
                    stitch_handler.S3_THRESHOLD_BYTES + 1,
                    schema.schema,
                    schema.key_properties,
                    schema.bookmark_properties)

        actual = decode_transit(mock_post_s3.call_args_list[0][0][0])
        expected = {
            "money": Decimal("10.42"),
        }
        self.assertDictEqual(expected, dict(actual["body"]["data"]))
