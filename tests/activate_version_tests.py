import unittest
import target_stitch
from target_stitch import StitchHandler, finish_requests
import io
import json
import simplejson
from tests import FakeSession

try:
    from tests.gate_mocks import mock_out_of_order_all_200
except ImportError:
    from gate_mocks import mock_out_of_order_all_200


class ActivateVersion(unittest.TestCase):

    def setUp(self):
        token = None
        self.first_flush_error = None
        self.second_flush_error = None

        handler = StitchHandler(target_stitch.DEFAULT_MAX_BATCH_BYTES, 1)

        self.out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [handler], self.out, 4000000, 1, 100000)
        self.queue = [simplejson.dumps({"type": "SCHEMA", "stream": "chicken_stream",
                                  "key_properties": ["my_float"],
                                  "schema": {"type": "object",
                                             "properties": {"my_float": {"type": "number"}}}})]
        target_stitch.SEND_EXCEPTION = None
        target_stitch.PENDING_REQUESTS = []
        self.og_flush_states = StitchHandler.flush_states
        # self.flushed_state_count = 0
        # StitchHandler.flush_states = self.fake_flush_states

        target_stitch.CONFIG = {
            'token': "some-token",
            'client_id': "some-client",
            'disable_collection': True,
            'connection_ns': "some-ns",
            'batch_size_preferences' : {
                'full_table_streams' : [],
                'batch_size_preference': None,
                'user_batch_size_preference': None,
            },
            'turbo_boost_factor' : 10,
            'small_batch_url' : "http://small-batch",
            'big_batch_url' : "http://big-batch",
        }

    def test_activate_version_finishes_pending_requests(self):
        target_stitch.OUR_SESSION = FakeSession(mock_out_of_order_all_200)
        # request 2 would ordinarily complete first because the mock_out_of_order_all_200, because
        # request 2 contains an ACTIVATE_VERSION, it will not even be sent until request 1 completes

        self.queue.append(
            json.dumps({"type": "STATE", "value": {"bookmarks": {"chicken_stream": {"id": 0}}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "version": 1,
                                      "record": {"id": 1, "name": "Mike"}}))
        # will flush here after 1 record
        self.queue.append(
            json.dumps({"type": "STATE", "value": {"bookmarks": {"chicken_stream": {"id": 1}}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", 'version': 1,
                                      "record": {"id": 2, "name": "Paul"}}))
        # will flush here after 1 record1
        self.queue.append(
            json.dumps({"type": "STATE", "value": {"bookmarks": {"chicken_stream": {"id": 2}}}}))
        self.queue.append(
            json.dumps({"type": "ACTIVATE_VERSION", 'stream': 'chicken_stream', 'version': 1}))
        # will flush here after 1 record1

        self.target_stitch.consume(self.queue)
        finish_requests()

        # there should be three flushes
        self.assertEqual(len(target_stitch.OUR_SESSION.bodies_completed), 3)

        # the last flush should include the activate version message
        self.assertEqual([m['action'] for m
                          in json.loads(
                                target_stitch.OUR_SESSION.bodies_completed[-1])['messages']],
                         ['activate_version'])

        # assert that the final state record is id 2
        emitted_state = list(map(lambda x: simplejson.loads(x, use_decimal=True),
                                 self.out.getvalue().strip().split('\n')))
        self.assertEqual(emitted_state[-1], {"bookmarks": {"chicken_stream": {"id": 2}}})


if __name__== "__main__":
    test1 = ActivateVersion()
    test1.setUp()
    test1.test_activate_version_finishes_pending_requests()
    #test1.test_unparseable_json_response()
