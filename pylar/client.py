"""
A client class.
"""


import asyncio
import json

from azmq.common import AsyncTaskObject
from functools import partial
from itertools import count

from .errors import raise_on_error


class Client(AsyncTaskObject):
    def __init__(self, *, socket, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self._request_id_generator = count()
        self._pending_requests = {}

    async def on_close(self):
        for future in self._pending_requests.values():
            future.cancel()

        await super().on_close()

    async def on_run(self):
        while True:
            reply = await self.socket.recv_multipart()

            try:
                reply.pop(0)  # Empty frame.
                type_ = reply.pop(0)
            except IndexError:
                continue

            if type_ == b'out':
                try:
                    request_id = reply.pop(0)
                except IndexError:
                    continue

                future = self._pending_requests.get(request_id)

                if future:
                    future.set_result(reply)
            elif type_ == b'in':
                print(reply)


    def get_request_id(self):
        return ('%s' % next(self._request_id_generator)).encode('utf-8')

    async def _request(self, command, *args):
        request_id = self.get_request_id()

        await self.socket.send_multipart((
            b'',
            b'out',
            request_id,
            command,
        ) + args)
        future = asyncio.Future(loop=self.loop)

        def request_done(_, client, request_id):
            client._pending_requests.pop(request_id)

        future.add_done_callback(
            partial(request_done, client=self, request_id=request_id),
        )

        self._pending_requests[request_id] = future
        reply = await future
        return raise_on_error(command=command, reply=reply)

    async def register(self, service_name):
        await self._request(
            b'register',
            service_name.encode('utf-8'),
        )

    async def unregister(self, service_name):
        await self._request(
            b'unregister',
            service_name.encode('utf-8'),
        )

    async def call(self, service_name, method, args=None, kwargs=None):
        reply = await self._request(
            b'call',
            service_name.encode('utf-8'),
            method.encode('utf-8'),
            json.dumps(args or [], separators=(',', ':')).encode('utf-8'),
            json.dumps(kwargs or {}, separators=(',', ':')).encode('utf-8'),
        )
        return json.loads(reply[0].decode('utf-8'))
