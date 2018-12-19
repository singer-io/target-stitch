import io
import json
import unittest

import singer
import target_stitch


class DummyHandler:
    def __init__(self):
        self.batches = []
        
    def handle_batch(self, messages, *args, **kwargs):
        self.batches.append(messages)


def mk_schema(stream):
    return json.dumps({
        "type": "SCHEMA",
        "stream": stream,
        "key_properties": ["name"],
        "schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
            },
        },
    })


def mk_version(stream, version):
    return json.dumps({
        "type": "ACTIVATE_VERSION",
        "stream": stream,
        "version": version,
    })


def _mk_record(stream, record):
    return json.dumps({
        "type": "RECORD",
        "stream": stream,
        "record": record,
    })


def mk_record(stream, num, version=None):
    msg = {
        "type": "RECORD",
        "stream": stream,
        "record": {
            "name": "{}-{}".format(stream, num)
        },
    }
    if version:
        msg["version"] = version

    return json.dumps(msg)


def mk_state(value):
    return json.dumps({
        "type": "STATE",
        "value": value,
    })


class TestTarget(unittest.TestCase):
    def setUp(self):
        self.handler = DummyHandler()
        self.state_writer = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch([self.handler], self.state_writer, 30)

    def test_schema_registration(self):
        self.target_stitch.consume([
            mk_schema("test1"),
            mk_schema("test2"),
        ])
        self.assertIn("test1", self.target_stitch.stream_meta)
        self.assertIn("test2", self.target_stitch.stream_meta)

    def test_flush_on_stream_change(self):
        test1_records = [mk_record("test1", i) for i in range(10)]
        test2_records = [mk_record("test2", i) for i in range(10)]

        lines = []
        lines.append(mk_schema("test1"))
        lines.append(mk_schema("test2"))
        lines.extend(test1_records)
        lines.extend(test2_records)
        self.target_stitch.consume(lines)

        expected = [
            [singer.parse_message(r) for r in test1_records],
            [singer.parse_message(r) for r in test2_records],
        ]
        self.assertEqual(expected, self.handler.batches)
        self.assertEqual([], self.target_stitch.messages)

    def test_flush_on_version_change(self):
        version1_records = [mk_record("test1", i, 1) for i in range(10)]
        version2_records = [mk_record("test1", i, 2) for i in range(10)]

        lines = []
        lines.append(mk_schema("test1"))
        lines.extend(version1_records)
        lines.extend(version2_records)
        self.target_stitch.consume(lines)

        expected = [
            [singer.parse_message(r) for r in version1_records],
            [singer.parse_message(r) for r in version2_records],
        ]
        self.assertEqual(expected, self.handler.batches)
        self.assertEqual([], self.target_stitch.messages)

    def test_flush_on_max_bytes(self):
        bytes_per_flush = target_stitch.MAX_BYTES_PER_FLUSH
        target_stitch.MAX_BYTES_PER_FLUSH = 1
        test1_records = [mk_record("test1", i) for i in range(10)]

        lines = []
        lines.append(mk_schema("test1"))
        lines.extend(test1_records)
        self.target_stitch.consume(lines)

        expected = [[singer.parse_message(r)] for r in test1_records]
        self.assertEqual(expected, self.handler.batches)
        self.assertEqual([], self.target_stitch.messages)
        target_stitch.MAX_BYTES_PER_FLUSH = bytes_per_flush

    def test_flush_on_max_records(self):
        records_per_flush = target_stitch.MAX_RECORDS_PER_FLUSH
        target_stitch.MAX_RECORDS_PER_FLUSH = 1
        test1_records = [mk_record("test1", i) for i in range(10)]

        lines = []
        lines.append(mk_schema("test1"))
        lines.extend(test1_records)
        self.target_stitch.consume(lines)

        expected = [[singer.parse_message(r)] for r in test1_records]
        self.assertEqual(expected, self.handler.batches)
        self.assertEqual([], self.target_stitch.messages)
        target_stitch.MAX_RECORDS_PER_FLUSH = records_per_flush

    def test_state_flush(self):
        records_per_flush = target_stitch.MAX_RECORDS_PER_FLUSH
        target_stitch.MAX_RECORDS_PER_FLUSH = 2

        lines = [
            mk_schema("test1"),
            mk_record("test1", 1),
            mk_record("test1", 2), # flush, no state
            mk_state(2),
            mk_record("test1", 3),
            mk_state(3),
            mk_record("test1", 4), # flush, state = 3
            mk_record("test1", 5),
            mk_record("test1", 6), # flush, no state changed
            mk_record("test1", 7),
            mk_state(7),
        ]
        self.target_stitch.consume(lines) # flush, state = 7

        self.assertEqual("3\n7\n", self.state_writer.getvalue())
        target_stitch.MAX_RECORDS_PER_FLUSH = records_per_flush

    def test_record_too_large(self):
        lines = [_mk_record("test1", {"name": "a" * target_stitch.MAX_BYTES_PER_RECORD})]
        with self.assertRaises(ValueError):
            self.target_stitch.consume(lines)
