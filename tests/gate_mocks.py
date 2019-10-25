import asyncio

def mock_unparsable_response_body_200(requests_sent):
    class FakeResponse:
        def __init__(self, requests_sent):
            self.requests_sent = requests_sent

        async def json(self):
            self.status = 200
            raise Exception("bad json response")

    return FakeResponse(requests_sent)

def mock_in_order_all_200(requests_sent):
    class FakeResponse:
        def __init__(self, requests_sent):
            self.requests_sent = requests_sent

        async def json(self):
            self.status = 200
            await asyncio.sleep(0)
            return {"status" : "finished request {}".format(requests_sent)}

    return FakeResponse(requests_sent)

def mock_out_of_order_all_200(requests_sent):
    class FakeResponse:
        def __init__(self, requests_sent):
            self.requests_sent = requests_sent

        async def json(self):
            self.status = 200
            if self.requests_sent == 1:
                await asyncio.sleep(3)
            return {"status" : "finished request {}".format(requests_sent)}

    return FakeResponse(requests_sent)

def mock_in_order_first_errors(requests_sent):
    class FakeResponse:
        def __init__(self, requests_sent):
            self.requests_sent = requests_sent

        async def json(self):
            if (self.requests_sent == 1):
                self.status = 400
                return {"status" : "finished request {}".format(requests_sent)}

            self.status = 200
            return {"status" : "finished request {}".format(requests_sent)}

    return FakeResponse(requests_sent)

def mock_in_order_second_errors(requests_sent):
    class FakeResponse:
        def __init__(self, requests_sent):
            self.requests_sent = requests_sent

        async def json(self):
            if (self.requests_sent == 2):
                self.status = 400
                return {"status" : "finished request {}".format(requests_sent)}

            self.status = 200
            return {"status" : "finished request {}".format(requests_sent)}

    return FakeResponse(requests_sent)

def mock_out_of_order_first_errors(requests_sent):
    class FakeResponse:
        def __init__(self, requests_sent):
            self.requests_sent = requests_sent

        async def json(self):
            if (self.requests_sent == 1):
                self.status = 400
                await asyncio.sleep(3)
                return {"status" : "finished request {}".format(requests_sent)}

            self.status = 200
            return {"status" : "finished request {}".format(requests_sent)}

    return FakeResponse(requests_sent)

def mock_out_of_order_second_errors(requests_sent):
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

def mock_out_of_order_both_error(requests_sent):
    class FakeResponse:
        def __init__(self, requests_sent):
            self.requests_sent = requests_sent

        async def json(self):
            self.status = 400
            if (self.requests_sent == 1):
                await asyncio.sleep(10)
                return {"status" : "finished request {}".format(requests_sent)}

            return {"status" : "finished request {}".format(requests_sent)}

    return FakeResponse(requests_sent)


def mock_in_order_both_error(requests_sent):
    class FakeResponse:
        def __init__(self, requests_sent):
            self.requests_sent = requests_sent

        async def json(self):
            self.status = 400
            return {"status" : "finished request {}".format(requests_sent)}

    return FakeResponse(requests_sent)
