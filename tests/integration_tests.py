import unittest
import target_stitch
from target_stitch import StitchHandler, TargetStitchException, DEFAULT_STITCH_URL
import io
import os

def load_sample_lines(filename):
    with open('tests/' + filename) as fp:
        return [line for line in fp]

def token():
    token = os.getenv('TARGET_STITCH_TEST_TOKEN')
    if not token:
        raise Exception('Integration tests require TARGET_STITCH_TEST_TOKEN environment variable to be set')
    return token

class IntegrationTest(unittest.TestCase):
    def setUp(self):
        handler = StitchHandler(token(), DEFAULT_STITCH_URL, 4000000)
        out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [handler], out, 4000000, 20000, 100000)

class TestRecordWithNullKeyProperty(IntegrationTest):

    def test(self):
        queue = load_sample_lines('record_missing_key_property.json')
        pattern = ('Error persisting data for table '
                   '"test_record_missing_key_property": '
                   'Record is missing key property id')
        with self.assertRaisesRegex(TargetStitchException, pattern):
            self.target_stitch.consume(queue)

class TestNoToken(unittest.TestCase):

    def setUp(self):
        token = None
        handler = StitchHandler(token, DEFAULT_STITCH_URL, 4000000)
        out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [handler], out, 4000000, 20000, 100000)

    def test(self):
        queue = load_sample_lines('record_missing_key_property.json')
        pattern = 'Not Authorized'
        with self.assertRaisesRegex(TargetStitchException, pattern):
            self.target_stitch.consume(queue)
