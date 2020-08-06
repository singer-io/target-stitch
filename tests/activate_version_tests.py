import unittest
import target_stitch
from target_stitch import StitchHandler, TargetStitchException, finish_requests
import io
import json
import simplejson
import asyncio

try:
    from tests.gate_mocks import mock_out_of_order_all_200
except ImportError:
    from gate_mocks  import mock_out_of_order_all_200


class FakePost:
    def __init__(self, requests_sent, makeFakeResponse):
        self.requests_sent = requests_sent
        self.makeFakeResponse = makeFakeResponse

    async def __aenter__(self):
        return self.makeFakeResponse(self.requests_sent)

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(1)

class FakeSession:
    def __init__(self, makeFakeResponse):
        self.requests_sent = 0
        self.bodies_sent = []
        self.makeFakeResponse = makeFakeResponse

    def post(self, url, *, data, **kwargs):
        self.requests_sent = self.requests_sent + 1
        self.bodies_sent.append(data)
        return FakePost(self.requests_sent, self.makeFakeResponse)

class ActivateVersion(unittest.TestCase):
    def fake_flush_states(self, state_writer, future):
        self.flushed_state_count = self.flushed_state_count + 1

        if self.flushed_state_count == 1:
            #2nd request has not begun because it contains an ActivateVersion and must wait for 1 to complete
            if len(target_stitch.PENDING_REQUESTS) != 1:
                self.first_flush_error = "ActivateVersion request should not have been issues until 1st request completed: wrong pending request count for first flush"

            if future != target_stitch.PENDING_REQUESTS[0][0]:
                self.first_flush_error = "ActivateVersion request should not have been issues until 1st request completed: received wrong future for first flush"

            if  target_stitch.PENDING_REQUESTS[0][1] != {'bookmarks': {'chicken_stream': {'id': 1}}}:
                self.first_flush_error = "ActivateVersion request should not have been issues until 1st request completed: wrong state for first flush"

        elif self.flushed_state_count == 2:
            if len(target_stitch.PENDING_REQUESTS) != 1:
                self.second_flush_error = "ActivateVersion request should not have been issues until 1st request completed: wrong pending request count for second flush"

            if future != target_stitch.PENDING_REQUESTS[0][0]:
                self.second_flush_error = "ActivateVersion request should not have been issues until 1st request completed: wrong future for second flush"

            if target_stitch.PENDING_REQUESTS[0][1] is not None:
                self.second_flush_error = "ActivateVersion request should not have been issues until 1st request completed: wrong state for second flush"

        else:
            raise Exception('flushed state should only have been called twice')

        self.og_flush_states(state_writer, future)


    def setUp(self):
        token = None
        self.first_flush_error = None
        self.second_flush_error = None

        handler = StitchHandler(target_stitch.DEFAULT_MAX_BATCH_BYTES, 2)

        self.out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [handler], self.out, 4000000, 2, 100000)
        self.queue = [simplejson.dumps({"type": "SCHEMA", "stream": "chicken_stream",
                                  "key_properties": ["my_float"],
                                  "schema": {"type": "object",
                                             "properties": {"my_float": {"type": "number"}}}})]
        target_stitch.SEND_EXCEPTION = None
        target_stitch.PENDING_REQUESTS = set()
        self.og_flush_states = StitchHandler.flush_states
        self.flushed_state_count = 0
        StitchHandler.flush_states = self.fake_flush_states

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
        #request 2 would ordinarily complete first because the mock_out_of_order_all_200, but because
        #request 2 contains an ACTIVATE_VERSION, it will not even be sent until request 1 completes

        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "version":1, "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        #will flush here after 2 records
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", 'version':1, "record": {"id": 2, "name": "Paul"}}))
        self.queue.append(json.dumps({"type":"ACTIVATE_VERSION", 'stream': 'chicken_stream', 'version': 1 }))
        #will flush here after 2 records


        self.target_stitch.consume(self.queue)
        finish_requests()
        self.assertEqual(self.first_flush_error, None, self.first_flush_error)
        self.assertEqual(self.second_flush_error, None, self.second_flush_error)


if __name__== "__main__":
    test1 = ActivateVersion()
    test1.setUp()
    test1.test_activate_version_finishes_pending_requests()
    #test1.test_unparseable_json_response()
