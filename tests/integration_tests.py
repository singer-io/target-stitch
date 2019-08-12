import unittest
import singer
import target_stitch
from target_stitch import StitchHandler, TargetStitchException, DEFAULT_STITCH_URL, finish_requests
import io
import os
import json
import asyncio
import simplejson
from decimal import Decimal
try:
    from tests.gate_mocks import mock_in_order_all_200, mock_out_of_order_all_200, mock_in_order_first_errors, mock_in_order_second_errors, mock_out_of_order_first_errors, mock_out_of_order_second_errors, mock_out_of_order_both_error, mock_in_order_both_error
except ImportError:
    from gate_mocks  import mock_in_order_all_200, mock_out_of_order_all_200, mock_in_order_first_errors, mock_in_order_second_errors, mock_out_of_order_first_errors, mock_out_of_order_second_errors, mock_out_of_order_both_error, mock_in_order_both_error

from nose.tools import nottest

LOGGER = singer.get_logger().getChild('target_stitch')

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
        self.bodies_sent = []
        self.makeFakeResponse = makeFakeResponse

    def post(self, url, *, data, **kwargs):
        self.requests_sent = self.requests_sent + 1
        self.bodies_sent.append(data)
        return FakePost(self.requests_sent, self.makeFakeResponse)


class AsyncSerializeFloats(unittest.TestCase):
    def setUp(self):
        token = None
        handler = StitchHandler(token,
                                DEFAULT_STITCH_URL,
                                target_stitch.DEFAULT_MAX_BATCH_BYTES,
                                2,
                                10)
        self.out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [handler], self.out, 4000000, 2, 100000)
        self.queue = [simplejson.dumps({"type": "SCHEMA", "stream": "chicken_stream",
                                  "key_properties": ["my_float"],
                                  "schema": {"type": "object",
                                             "properties": {"my_float": {"type": "number"}}}})]
        target_stitch.SEND_EXCEPTION = None
        target_stitch.PENDING_REQUESTS = []

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
        handler = StitchHandler(token,
                                DEFAULT_STITCH_URL,
                                target_stitch.DEFAULT_MAX_BATCH_BYTES,
                                2,
                                10)
        self.out = io.StringIO()
        self.target_stitch = target_stitch.TargetStitch(
            [handler], self.out, 4000000, 2, 100000)
        self.queue = [json.dumps({"type": "SCHEMA", "stream": "chicken_stream",
                                  "key_properties": ["id"],
                                  "schema": {"type": "object",
                                             "properties": {"id": {"type": "integer"},
                                                            "name": {"type": "string"}}}})]
        target_stitch.SEND_EXCEPTION = None
        target_stitch.PENDING_REQUESTS = []

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

        self.target_stitch.consume(self.queue)
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

        self.target_stitch.consume(self.queue)
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

        self.target_stitch.consume(self.queue)
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

        self.target_stitch.consume(self.queue)
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

        self.target_stitch.consume(self.queue)
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

        self.target_stitch.consume(self.queue)
        our_exception = None
        try:
            finish_requests()
        except Exception as ex:
            our_exception = ex

        self.assertIsNotNone(our_exception)
        self.assertTrue(isinstance(our_exception, TargetStitchException))

        #no state is emitted
        self.assertEqual(self.out.getvalue(), '')


if __name__== "__main__":
    test1 = AsyncPushToGate()
    test1.setUp()
    test1.test_requests_in_order_both_errors()
