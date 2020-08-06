import unittest
import singer
import target_stitch
from target_stitch import StitchHandler, TargetStitchException, finish_requests
import io
import os
import json
import asyncio
import simplejson
from decimal import Decimal
try:
    from tests.gate_mocks import (
        mock_in_order_all_200,
        mock_out_of_order_all_200,
        mock_in_order_first_errors,
        mock_in_order_second_errors,
        mock_out_of_order_first_errors,
        mock_out_of_order_second_errors,
        mock_out_of_order_both_error,
        mock_in_order_both_error,
        mock_unparsable_response_body_200,
    )
except ImportError:
    from gate_mocks import (
        mock_in_order_all_200,
        mock_out_of_order_all_200,
        mock_in_order_first_errors,
        mock_in_order_second_errors,
        mock_out_of_order_first_errors,
        mock_out_of_order_second_errors,
        mock_out_of_order_both_error,
        mock_in_order_both_error,
        mock_unparsable_response_body_200,
    )

from nose.tools import nottest

LOGGER = singer.get_logger().getChild('target_stitch')

def fake_check_send_exception():
    return None

def load_sample_lines(filename):
    with open('tests/' + filename) as fp:
        return [line for line in fp]

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
        self.urls = []
        self.messages_sent = []
        self.bodies_sent = []
        self.makeFakeResponse = makeFakeResponse

    def post(self, url, *, data, **kwargs):
        data_json = simplejson.loads(data)
        self.messages_sent.append(data_json["messages"])
        self.requests_sent = self.requests_sent + 1
        self.bodies_sent.append(data)
        self.urls.append(url)
        return FakePost(self.requests_sent, self.makeFakeResponse)


class AsyncSerializeFloats(unittest.TestCase):
    def setUp(self):
        token = None
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

        LOGGER.info("cleaning SEND_EXCEPTIONS: %s AND PENDING_REQUESTS: %s",
                    target_stitch.SEND_EXCEPTION,
                    target_stitch.PENDING_REQUESTS)

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


    def test_serialize_floats(self):
        floats = [
            '-9999999999999999.9999999999999999999999',
            '-7187498962233394.3739812942138415666763',
            '9273972760690975.2044306442955715221042',
            '29515565286974.1188802122612813004366',
            '9176089101347578.2596296292040288441238',
            '-8416853039392703.306423225471199148379',
            '1285266411314091.3002668125515694162268',
            '6051872750342125.3812886238958681227336',
            '-1132031605459408.5571559429308939781468',
            '-6387836755056303.0038029604189860431045',
            '4526059300505414'
        ]

        target_stitch.OUR_SESSION = FakeSession(mock_in_order_all_200)
        for float_val in floats:
            self.queue.append(simplejson.dumps({"type": "RECORD",
                                                "stream": "chicken_stream",
                                                "record": {"my_float": Decimal(float_val)}}))


            self.queue.append(simplejson.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"my_float": Decimal(float_val) }}}}))

        self.target_stitch.consume(self.queue)
        finish_requests()

        output_record_floats = []
        for batch in target_stitch.OUR_SESSION.bodies_sent:
            output_record_floats.extend([str(x['data']['my_float']) for x in simplejson.loads(batch, use_decimal=True)['messages']])

        self.assertEqual(floats, output_record_floats)

        emitted_state = list(map(lambda x: simplejson.loads(x, use_decimal=True), self.out.getvalue().strip().split('\n')))
        self.assertEqual(len(emitted_state), 6)
        self.assertEqual( emitted_state[0], {'bookmarks': {'chicken_stream': {'my_float': Decimal(floats[0])}}})
        self.assertEqual( emitted_state[1], {'bookmarks': {'chicken_stream': {'my_float': Decimal(floats[2])}}})
        self.assertEqual( emitted_state[2], {'bookmarks': {'chicken_stream': {'my_float': Decimal(floats[4])}}})
        self.assertEqual( emitted_state[3], {'bookmarks': {'chicken_stream': {'my_float': Decimal(floats[6])}}})
        self.assertEqual( emitted_state[4], {'bookmarks': {'chicken_stream': {'my_float': Decimal(floats[8])}}})
        self.assertEqual( emitted_state[5], {'bookmarks': {'chicken_stream': {'my_float': Decimal(floats[10])}}})


class AsyncPushToGate(unittest.TestCase):
    def setUp(self):
        token = None
        handler = StitchHandler(target_stitch.DEFAULT_MAX_BATCH_BYTES, 2)

        self.og_check_send_exception = target_stitch.check_send_exception
        self.out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [handler], self.out, 4000000, 2, 100000)
        self.queue = [json.dumps({"type": "SCHEMA", "stream": "chicken_stream",
                                  "key_properties": ["id"],
                                  "schema": {"type": "object",
                                             "properties": {"id": {"type": "integer"},
                                                            "name": {"type": "string"}}}})]

        target_stitch.SEND_EXCEPTION = None
        for f in target_stitch.PENDING_REQUESTS:
            try:
                f.cancel()
            except:
                pass

        target_stitch.PENDING_REQUESTS = set()
        LOGGER.info("cleaning SEND_EXCEPTIONS: %s AND PENDING_REQUESTS: %s",
                    target_stitch.SEND_EXCEPTION,
                    target_stitch.PENDING_REQUESTS)

        target_stitch.CONFIG ={
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

    # 2 requests
    # both with state
    # in order responses
    def test_requests_in_order(self):
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_all_200)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 2 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 3 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Cathy"}}))
        #will flush here after 2 records

        self.target_stitch.consume(self.queue)
        finish_requests()

        emitted_state = list(map(json.loads, self.out.getvalue().strip().split('\n')))
        self.assertEqual(len(emitted_state), 2)
        self.assertEqual( emitted_state[0], {'bookmarks': {'chicken_stream': {'id': 1}}})
        self.assertEqual( emitted_state[1], {'bookmarks': {'chicken_stream': {'id': 3}}})

    def test_request_to_big_batch_for_large_record(self):
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_all_200)
        self.target_stitch.max_batch_records = 4
        self.target_stitch.handlers[0].max_batch_records = 4
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "M" * 5000000}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Cathy"}}))
        #will flush here after 4 records

        self.target_stitch.consume(self.queue)
        finish_requests()
        self.assertEqual(target_stitch.OUR_SESSION.urls, [target_stitch.CONFIG["big_batch_url"],
                                                          target_stitch.CONFIG["small_batch_url"]])
        self.assertEqual(len(target_stitch.OUR_SESSION.messages_sent[0]), 1)
        self.assertEqual(len(target_stitch.OUR_SESSION.messages_sent[1]), 3)

    # 2 requests
    # last SENT request has state
    # in order
    def test_requests_in_order_first_has_no_state(self):
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_all_200)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 2 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 3 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Cathy"}}))
        #will flush here after 2 records

        self.target_stitch.consume(self.queue)
        finish_requests()

        emitted_state = list(map(json.loads, self.out.getvalue().strip().split('\n')))
        self.assertEqual(len(emitted_state), 1)
        self.assertEqual( emitted_state[0], {'bookmarks': {'chicken_stream': {'id': 3}}})


    # 2 requests.
    # both with state.
    # in order
    # first sent request errors
    def test_requests_in_order_first_errors(self):
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_first_errors)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 2 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 3 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Cathy"}}))
        #will flush here after 2 records

        #consume() can encounter an exception via check_send_exception in send()
        #if SEND_EXCEPTION has already been set by the coroutine it can blow up.
        target_stitch.check_send_exception = fake_check_send_exception
        self.target_stitch.consume(self.queue)
        target_stitch.check_send_exception = self.og_check_send_exception
        our_exception = None
        try:
            finish_requests()
        except Exception as ex:
            our_exception = ex

        self.assertIsNotNone(our_exception)
        self.assertTrue(isinstance(our_exception, TargetStitchException))

        #no state is emitted
        emitted_state = self.assertEqual(self.out.getvalue(), '')

    # 2 requests.
    # both with state.
    # in order
    # second SENT request errors
    def test_requests_in_order_second_errors(self):
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_second_errors)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 2 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 3 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Cathy"}}))
        #will flush here after 2 records

        #consume() can encounter an exception via check_send_exception in send()
        #if SEND_EXCEPTION has already been set by the coroutine it can blow up.
        target_stitch.check_send_exception = fake_check_send_exception
        self.target_stitch.consume(self.queue)
        target_stitch.check_send_exception = self.og_check_send_exception

        our_exception = None
        try:
            finish_requests()
        except Exception as ex:
            our_exception = ex

        self.assertIsNotNone(our_exception)
        self.assertTrue(isinstance(our_exception, TargetStitchException))

        emitted_state = self.out.getvalue().strip().split('\n')
        self.assertEqual(1, len(emitted_state))
        self.assertEqual({'bookmarks': {'chicken_stream': {'id': 1}}}, json.loads(emitted_state[0]))

    # 2 requests.
    # both with state.
    # in order
    # both requests errors
    def test_requests_in_order_both_errors(self):
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_both_error)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 2 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 3 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Cathy"}}))
        #will flush here after 2 records

        #consume() can encounter an exception via check_send_exception in send()
        #if SEND_EXCEPTION has already been set by the coroutine it can blow up.
        target_stitch.check_send_exception = fake_check_send_exception
        self.target_stitch.consume(self.queue)
        target_stitch.check_send_exception = self.og_check_send_exception
        our_exception = None
        try:
            finish_requests()
        except Exception as ex:
            our_exception = ex

        self.assertIsNotNone(our_exception)
        self.assertTrue(isinstance(our_exception, TargetStitchException))

        #no state is emitted
        self.assertEqual(self.out.getvalue(), '')




    # 2 requests
    # both with state.
    # out of order responses
    def test_requests_out_of_order(self):
        target_stitch.OUR_SESSION = FakeSession(mock_out_of_order_all_200)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records

        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 2 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 3 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Cathy"}}))
        #will flush here after 2 records

        self.target_stitch.consume(self.queue)
        finish_requests()

        emitted_state = list(map(json.loads, self.out.getvalue().strip().split('\n')))
        self.assertEqual(len(emitted_state), 2)
        self.assertEqual( emitted_state[0], {'bookmarks': {'chicken_stream': {'id': 1}}})
        self.assertEqual( emitted_state[1], {'bookmarks': {'chicken_stream': {'id': 3}}})

    # 2 requests.
    # both with state.
    # out of order
    # first SENT request errors
    def test_requests_out_of_order_first_errors(self):
        target_stitch.OUR_SESSION = FakeSession(mock_out_of_order_first_errors)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 2 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 3 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Cathy"}}))
        #will flush here after 2 records

        #consume() can encounter an exception via check_send_exception in send()
        #if SEND_EXCEPTION has already been set by the coroutine it can blow up.
        target_stitch.check_send_exception = fake_check_send_exception
        self.target_stitch.consume(self.queue)
        target_stitch.check_send_exception = self.og_check_send_exception
        our_exception = None
        try:
            finish_requests()
        except Exception as ex:
            our_exception = ex

        self.assertIsNotNone(our_exception)
        self.assertTrue(isinstance(our_exception, TargetStitchException))

        #no state is emitted
        self.assertEqual(self.out.getvalue(), '')

    # 2 requests.
    # both with state.
    # out of order
    # second SENT request errors
    def out_of_order_second_errors(self, requests_sent):
        class FakeResponse:
            def __init__(self, requests_sent):
                self.requests_sent = requests_sent

            async def json(self):
                if (self.requests_sent == 1):
                    self.status = 200
                    await asyncio.sleep(3)
                    return {"status" : "finished request {}".format(requests_sent)}

                self.status = 400
                return {"status" : "finished request {}".format(requests_sent)}

        return FakeResponse(requests_sent)

    def test_requests_out_of_order_second_errors(self):
        target_stitch.OUR_SESSION = FakeSession(mock_out_of_order_second_errors)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 2 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 3 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Cathy"}}))
        #will flush here after 2 records

        #consume() can encounter an exception via check_send_exception in send()
        #if SEND_EXCEPTION has already been set by the coroutine it can blow up.
        target_stitch.check_send_exception = fake_check_send_exception
        self.target_stitch.consume(self.queue)
        target_stitch.check_send_exception = self.og_check_send_exception
        our_exception = None
        try:
            finish_requests()
        except Exception as ex:
            our_exception = ex

        #the 2nd request returns immediately with a 400, triggering a TargetStitchException.
        #at this point, it is game over and it does NOT matter when or with what status the 1st request comples
        self.assertIsNotNone(our_exception)
        self.assertTrue(isinstance(our_exception, TargetStitchException))

        emitted_state = self.out.getvalue().strip().split('\n')
        self.assertEqual(1, len(emitted_state))
        self.assertEqual('', emitted_state[0])

    # 2 requests.
    # both with state.
    # out of order
    # both requests errors
    def test_requests_out_of_order_both_errors(self):
        target_stitch.OUR_SESSION = FakeSession(mock_out_of_order_both_error)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 2 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 3 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Cathy"}}))
        #will flush here after 2 records

        #consume() can encounter an exception via check_send_exception in send()
        #if SEND_EXCEPTION has already been set by the coroutine it can blow up.
        target_stitch.check_send_exception = fake_check_send_exception
        self.target_stitch.consume(self.queue)
        target_stitch.check_send_exception = self.og_check_send_exception
        our_exception = None
        try:
            finish_requests()
        except Exception as ex:
            our_exception = ex

        self.assertIsNotNone(our_exception)
        self.assertTrue(isinstance(our_exception, TargetStitchException))

        #no state is emitted
        self.assertEqual(self.out.getvalue(), '')

    def test_unparseable_json_response(self):
        target_stitch.OUR_SESSION = FakeSession(mock_unparsable_response_body_200)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records

        target_stitch.check_send_exception = fake_check_send_exception
        self.target_stitch.consume(self.queue)
        target_stitch.check_send_exception = self.og_check_send_exception
        try:
            finish_requests()
        except Exception as ex:
            our_exception = ex

        self.assertIsNotNone(our_exception)


class StateOnly(unittest.TestCase):
    def setUp(self):
        token = None
        handler = StitchHandler(target_stitch.DEFAULT_MAX_BATCH_BYTES, 2)
        self.og_check_send_exception = target_stitch.check_send_exception
        self.out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [handler], self.out, 4000000, 1, 0)
        self.queue = []
        target_stitch.SEND_EXCEPTION = None
        for f in target_stitch.PENDING_REQUESTS:
            try:
                f.cancel()
            except:
                pass

        target_stitch.PENDING_REQUESTS = set()
        LOGGER.info("cleaning SEND_EXCEPTIONS: %s AND PENDING_REQUESTS: %s",
                    target_stitch.SEND_EXCEPTION,
                    target_stitch.PENDING_REQUESTS)
        target_stitch.CONFIG ={
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

    def test_state_only(self):
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_all_200)
        self.queue.append(json.dumps({"type":"STATE", "value":{"bookmarks":{"chicken_stream":{"id": 1 }}}}))
        #will flush here, because TargetStitch.time_last_batch_sent was set to 0 in setUp
        self.target_stitch.consume(self.queue)
        finish_requests()

        emitted_state = list(map(json.loads, self.out.getvalue().strip().split('\n')))
        self.assertEqual(len(emitted_state), 1)
        self.assertEqual( emitted_state[0], {'bookmarks': {'chicken_stream': {'id': 1}}})


class StateEdgeCases(unittest.TestCase):
    def setUp(self):
        token = None
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

        LOGGER.info("cleaning SEND_EXCEPTIONS: %s AND PENDING_REQUESTS: %s",
                    target_stitch.SEND_EXCEPTION,
                    target_stitch.PENDING_REQUESTS)

        target_stitch.CONFIG ={
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


    def test_trailing_state_after_final_message(self):
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_all_200)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE",
                                      "value":{"bookmarks":{"chicken_stream":{"id": 1 }},
                                               'currently_syncing' : 'chicken_stream'}}))

        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records
        self.queue.append(json.dumps({"type":"STATE",
                                      "value":{"bookmarks":{"chicken_stream":{"id": 2 }},
                                               'currently_syncing' : None}}))

        self.target_stitch.consume(self.queue)
        finish_requests()

        emitted_state = list(map(json.loads, self.out.getvalue().strip().split('\n')))
        self.assertEqual(len(emitted_state), 2)
        self.assertEqual( emitted_state[0],
                          {"bookmarks":{"chicken_stream":{"id": 1 }},
                           'currently_syncing' : 'chicken_stream'})
        self.assertEqual( emitted_state[1],
                          {"bookmarks":{"chicken_stream":{"id": 2 }},
                           'currently_syncing' : None})

    def test_will_not_output_empty_state(self):
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_all_200)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type":"STATE",
                                      "value":{"bookmarks":{"chicken_stream":{"id": 1 }},
                                               'currently_syncing' : 'chicken_stream'}}))

        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2, "name": "Paul"}}))
        #will flush here after 2 records, state will reset to None

        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Kyle"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 4, "name": "Alice"}}))
        #will flush here after 2 records, but will NOT write blank state

        self.target_stitch.consume(self.queue)
        finish_requests()

        emitted_state = list(map(json.loads, self.out.getvalue().strip().split('\n')))
        self.assertEqual(len(emitted_state), 1)
        self.assertEqual( emitted_state[0],
                          {"bookmarks":{"chicken_stream":{"id": 1 }},
                           'currently_syncing' : 'chicken_stream'})

class BufferingPerStreamConstraints(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        token = None
        handler = StitchHandler(target_stitch.DEFAULT_MAX_BATCH_BYTES, 3)

        self.og_check_send_exception = target_stitch.check_send_exception
        self.out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [handler], self.out, 500, 7, 100000)
        self.queue = [json.dumps({"type": "SCHEMA", "stream": "chicken_stream",
                                  "key_properties": ["id"],
                                  "schema": {"type": "object",
                                             "properties": {"id": {"type": "integer"},
                                                            "name": {"type": "string"}}}}),
                      json.dumps({"type": "SCHEMA", "stream": "zebra_stream",
                                  "key_properties": ["id"],
                                  "schema": {"type": "object",
                                             "properties": {"id": {"type": "integer"},
                                                            "name": {"type": "string"}}}})]

        target_stitch.SEND_EXCEPTION = None
        for f in target_stitch.PENDING_REQUESTS:
            try:
                f.cancel()
            except:
                pass

        target_stitch.PENDING_REQUESTS = set()

        LOGGER.info("cleaning SEND_EXCEPTIONS: %s AND PENDING_REQUESTS: %s",
                    target_stitch.SEND_EXCEPTION,
                    target_stitch.PENDING_REQUESTS)

        target_stitch.CONFIG ={
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

    def test_flush_based_on_message_count(self):
        # Tests that the target will buffer records per stream. This will
        # allow the tap to alternate which streams it is emitting records
        # for without the target cutting small batches
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_all_200)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "zebra_stream", "record": {"id": 2, "name": "Paul"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "zebra_stream", "record": {"id": 4, "name": "Cathy"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 5, "name": "Dan"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "zebra_stream", "record": {"id": 6, "name": "A"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 7, "name": "B"}}))
        # Should flush here
        self.queue.append(json.dumps({"type": "RECORD", "stream": "zebra_stream", "record": {"id": 8, "name": "C"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 9, "name": "D"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 10, "name": "E"}}))
        # Should flush here

        self.target_stitch.consume(self.queue)
        finish_requests()

        expected_messages = [
            [{'action': 'upsert',
              'data': {'id': 8, 'name': 'C'}}],
            [{'action': 'upsert',
              'data': {'id': 9, 'name': 'D'}},
             {'action': 'upsert',
              'data': {'id': 10, 'name': 'E'}}],
            [{'action': 'upsert',
              'data': {'id': 2, 'name': 'Paul'}},
             {'action': 'upsert',
              'data': {'id': 4, 'name': 'Cathy'}},
             {'action': 'upsert',
              'data': {'id': 6, 'name': 'A'}}],
            [{'action': 'upsert',
              'data': {'id': 1, 'name': 'Mike'}},
             {'action': 'upsert',
              'data': {'id': 3, 'name': 'Harrsion'}},
             {'action': 'upsert',
              'data': {'id': 5, 'name': 'Dan'}},
             {'action': 'upsert',
              'data': {'id': 7, 'name': 'B'}},]]

        # Should be broken into 4 batches
        self.assertEqual(len(target_stitch.OUR_SESSION.messages_sent), 4)

        # Sort by length and remove sequence number to compare directly
        actual_messages = [[{key: m[key] for key in ["action","data"]} for m in ms]
                           for ms in sorted(target_stitch.OUR_SESSION.messages_sent, key=lambda ms: len(ms))]
        self.assertEqual(actual_messages, expected_messages)

    def test_flush_based_on_bytes(self):
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_all_200)
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1, "name": "Mike"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "zebra_stream", "record": {"id": 2, "name": "Paul"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 3, "name": "Harrsion"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "zebra_stream", "record": {"id": 4, "name": "The byte limit should be across streams, so lets make lots of data on both streams"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 5, "name": "to force the target to exceed its byte limit"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "zebra_stream", "record": {"id": 6, "name": "A"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 7, "name": "B"}}))
        # Should flush here
        self.queue.append(json.dumps({"type": "RECORD", "stream": "zebra_stream", "record": {"id": 8, "name": "C"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 9, "name": "D"}}))
        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 10, "name": "E"}}))
        # Should flush here

        self.target_stitch.consume(self.queue)
        finish_requests()

        expected_messages = [
            [{'action': 'upsert', 'data': {'id': 1, 'name': 'Mike'}},
             {'action': 'upsert', 'data': {'id': 3, 'name': 'Harrsion'}},
             {'action': 'upsert',
              'data': {'id': 5,
                       'name': 'to force the target to exceed its byte limit'}}],
            [{'action': 'upsert', 'data': {'id': 2, 'name': 'Paul'}},
             {'action': 'upsert', 'data': {'id': 4, 'name': 'The byte limit should be across streams, so lets make lots of data on both streams'}}],
            [{'action': 'upsert', 'data': {'id': 6, 'name': 'A'}},
             {'action': 'upsert', 'data': {'id': 8, 'name': 'C'}}],
            [{'action': 'upsert', 'data': {'id': 7, 'name': 'B'}},
             {'action': 'upsert', 'data': {'id': 9, 'name': 'D'}},
             {'action': 'upsert', 'data': {'id': 10, 'name': 'E'}}]]

        # Should be broken into 4 batches
        self.assertEqual(len(target_stitch.OUR_SESSION.messages_sent), 4)

        # Sort by length and remove sequence number to compare directly
        actual_messages = [[{key: m[key] for key in ["action","data"]} for m in ms]
                           for ms in sorted(target_stitch.OUR_SESSION.messages_sent, key=lambda ms: ms[0]['data']['id'])]

        self.assertEqual(actual_messages, expected_messages)


class BufferingPerStreamState(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        token = None
        handler = StitchHandler(target_stitch.DEFAULT_MAX_BATCH_BYTES, 3)

        # Swap out the post_coroutine with a mocked one to fake failures
        self.actual_post_coroutine = target_stitch.post_coroutine
        target_stitch.post_coroutine = self.mock_post_coroutine

        self.messages_sent = 0

        self.og_check_send_exception = target_stitch.check_send_exception
        self.out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [handler], self.out, 4000000, 10, 100000)
        self.queue = [json.dumps({"type": "SCHEMA", "stream": "chicken_stream",
                                  "key_properties": ["id"],
                                  "schema": {"type": "object",
                                             "properties": {"id": {"type": "integer"}}}}),
                      json.dumps({"type": "SCHEMA", "stream": "zebra_stream",
                                  "key_properties": ["id"],
                                  "schema": {"type": "object",
                                             "properties": {"id": {"type": "integer"}}}}),
                      json.dumps({"type": "SCHEMA", "stream": "dog_stream",
                                  "key_properties": ["id"],
                                  "schema": {"type": "object",
                                             "properties": {"id": {"type": "integer"}}}})]

        target_stitch.SEND_EXCEPTION = None
        for f in target_stitch.PENDING_REQUESTS:
            try:
                f.cancel()
            except:
                pass

        target_stitch.PENDING_REQUESTS = set()
        LOGGER.info("cleaning SEND_EXCEPTIONS: %s AND PENDING_REQUESTS: %s",
                    target_stitch.SEND_EXCEPTION,
                    target_stitch.PENDING_REQUESTS)

        target_stitch.CONFIG ={
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


    def tearDown(self):
        target_stitch.post_coroutine = self.actual_post_coroutine

    async def mock_post_coroutine(self, url, headers, data, verify_ssl):
        self.messages_sent += 1
        LOGGER.info("MOCK POST COROUTINE: Sending message number %s", self.messages_sent)
        if self.messages_sent == 3:
            return await self.wait_then_throw()
        else:
            return await self.actual_post_coroutine(url, headers, data, verify_ssl)

    @staticmethod
    async def wait_then_throw():
        await asyncio.sleep(1)
        raise target_stitch.StitchClientResponseError(400, "Test exception")

    def test_state_interleaving(self):
        # Tests that the target will buffer records per stream. This will
        # allow the tap to alternate which streams it is emitting records
        # for without the target cutting small batches
        target_stitch.OUR_SESSION = FakeSession(mock_in_order_all_200)

        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 1}}))
        self.queue.append(json.dumps({"type": "STATE",  "value": {"bookmarks": {"chicken_stream": {"id": 1}}}}))

        self.queue.append(json.dumps({"type": "RECORD", "stream": "zebra_stream", "record": {"id": 1}}))
        self.queue.append(json.dumps({"type": "STATE",  "value": {"bookmarks": {"chicken_stream": {"id": 1},
                                                                                "zebra_stream": {"id": 1}}}}))

        self.queue.append(json.dumps({"type": "RECORD", "stream": "dog_stream", "record": {"id": 1}}))
        self.queue.append(json.dumps({"type": "STATE",  "value": {"bookmarks": {"chicken_stream": {"id": 1},
                                                                                "zebra_stream": {"id": 1},
                                                                                "dog_stream": {"id": 1}}}}))

        self.queue.append(json.dumps({"type": "RECORD", "stream": "chicken_stream", "record": {"id": 2}}))
        self.queue.append(json.dumps({"type": "STATE",  "value": {"bookmarks": {"chicken_stream": {"id": 2},
                                                                                "zebra_stream": {"id": 1},
                                                                                "dog_stream": {"id": 1}}}}))

        self.queue.append(json.dumps({"type": "RECORD", "stream": "zebra_stream", "record": {"id": 2}}))
        self.queue.append(json.dumps({"type": "STATE",  "value": {"bookmarks": {"chicken_stream": {"id": 2},
                                                                                "zebra_stream": {"id": 2},
                                                                                "dog_stream": {"id": 1}}}}))

        self.queue.append(json.dumps({"type": "RECORD", "stream": "dog_stream", "record": {"id": 2}}))
        self.queue.append(json.dumps({"type": "STATE",  "value": {"bookmarks": {"chicken_stream": {"id": 2},
                                                                                "zebra_stream": {"id": 2},
                                                                                "dog_stream": {"id": 2}}}}))


        try:
            self.target_stitch.consume(self.queue)
            finish_requests()
        except target_stitch.TargetStitchException as ex:
            our_exception = ex

        self.assertIsNotNone(our_exception)


        # There should only be messages for the 2 streams because the
        # third one should fail due to the mocking code
        expected_messages = [[{'action': 'upsert', 'data': {'id': 1}},
                              {'action': 'upsert', 'data': {'id': 2}}],
                             [{'action': 'upsert', 'data': {'id': 1}},
                             {'action': 'upsert', 'data': {'id': 2}}]]

        # We expect no state because one of the requests fails
        expected_state = ''

        # Should be broken into 2 batches (because the third fails)
        self.assertEqual(len(target_stitch.OUR_SESSION.messages_sent), 2)

        # Sort by length and remove sequence number to compare directly
        emitted_state = self.out.getvalue()
        actual_messages = [[{key: m[key] for key in ["action","data"]} for m in ms]
                           for ms in sorted(target_stitch.OUR_SESSION.messages_sent, key=lambda ms: len(ms))]

        self.assertEqual(actual_messages, expected_messages)
        self.assertEqual(emitted_state, expected_state)



if __name__== "__main__":
    test1 = StateEdgeCases()
    test1.setUp()
    test1.test_will_not_output_empty_state()
    # test1.test_requests_in_order()
