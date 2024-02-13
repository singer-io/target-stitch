import asyncio

import simplejson


class FakePost:
    def __init__(self, requests_sent, makeFakeResponse, bodies_completed, data):
        self.requests_sent = requests_sent
        self.makeFakeResponse = makeFakeResponse
        self.bodies_completed = bodies_completed
        self.data = data

    async def __aenter__(self):
        return self.makeFakeResponse(self.requests_sent)

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.sleep(1)
        self.bodies_completed.append(self.data)


class FakeSession:
    def __init__(self, makeFakeResponse):
        self.requests_sent = 0
        self.requests_completed = 0
        self.urls = []
        self.messages_sent = []
        self.bodies_sent = []
        self.bodies_completed = []
        self.makeFakeResponse = makeFakeResponse

    def post(self, url, *, data, **kwargs):
        data_json = simplejson.loads(data)
        self.messages_sent.append(data_json["messages"])
        self.requests_sent = self.requests_sent + 1
        self.bodies_sent.append(data)
        self.urls.append(url)
        return FakePost(self.requests_sent, self.makeFakeResponse, self.bodies_completed, data)