"""
A client class.
"""


import asyncio
import json

from azmq.common import AsyncTaskObject
from functools import partial
from itertools import count

from .common import Requester
from .errors import raise_on_error


class Client(AsyncTaskObject):
    def __init__(self, *, socket, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self._requester = Requester(
            loop=self.loop,
            send_request_callback=self.__send_request,
        )

    async def on_close(self):
        self._requester.cancel_pending_requests()

        await super().on_close()

    async def on_run(self):
        while True:
            reply = await self.socket.recv_multipart()

            try:
                reply.pop(0)  # Empty frame.
                domain = reply.pop(0)
            except IndexError:
                continue

            if domain == b'broker':
                try:
                    request_id = reply.pop(0)
                except IndexError:
                    continue

                self._requester.set_request_result(request_id, reply)

            elif domain == b'service':
                print("received", reply)

    async def request(self, command, *args):
        result = await self._requester.request(command, args)
        return raise_on_error(command, result)

    async def register(self, service_name):
        await self.request(
            b'register',
            service_name.encode('utf-8'),
        )

    async def unregister(self, service_name):
        await self.request(
            b'unregister',
            service_name.encode('utf-8'),
        )

    async def call(self, service_name, method, args=None, kwargs=None):
        reply = await self.request(
            b'call',
            service_name.encode('utf-8'),
            method.encode('utf-8'),
            json.dumps(args or [], separators=(',', ':')).encode('utf-8'),
            json.dumps(kwargs or {}, separators=(',', ':')).encode('utf-8'),
        )
        print(reply)
        return json.loads(reply[0].decode('utf-8'))

    # Private methods.

    async def __send_request(self, command, args, *, request_id):
        await self.socket.send_multipart((
            b'',
            b'broker',
            request_id,
            command,
        ) + args)
