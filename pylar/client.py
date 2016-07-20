"""
A client class.
"""


import asyncio
import json

from azmq.common import AsyncTaskObject
from functools import partial
from itertools import (
    count,
    chain,
)

from .common import (
    Requester,
    deserialize,
    serialize,
)
from .errors import raise_on_error


class Client(AsyncTaskObject):
    def __init__(self, *, socket, domain, credentials, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self.domain = domain
        self.credentials = credentials
        self._requester = Requester(
            loop=self.loop,
            send_request_callback=self.__send_request,
        )

    async def register(self):
        await self._request(
            (b'register',),
            self.domain,
            (b'',),
            self.credentials,
        )

    async def unregister(self):
        await self._request(
            (b'unregister',),
        )

    async def call(self, domain, method, args=None, kwargs=None):
        reply = await self._request(
            (b'call',),
            domain,
            (
                b'',
                method.encode('utf-8'),
                serialize(args or []),
                serialize(kwargs or {}),
            ),
        )
        return deserialize(reply[0])

    # Internal methods.

    async def on_close(self):
        self._requester.cancel_pending_requests()

        await super().on_close()

    async def on_run(self):
        while True:
            reply = await self.socket.recv_multipart()

            try:
                reply.pop(0)  # Empty frame.
                type_ = reply.pop(0)
                request_id = reply.pop(0)
            except IndexError:
                continue

            if type_ == b'request':
                # TODO: Implement for real.
                await self.__send_response(
                    request_id=request_id,
                    args=(serialize(42),),
                )

            elif type_ == b'response':
                self._requester.set_request_result(request_id, reply)

    async def _request(self, *args):
        result = await self._requester.request(args=chain(*args))
        return raise_on_error(result)

    async def _response(self, request_id, code, args):
        await self.__send_response(
            request_id,
            [
                ('%d' % code).encode('utf-8'),
            ] + list(args),
        )

    # Private methods.

    async def __send_request(self, *, request_id, args):
        frames = [
            b'',
            b'request',
            request_id,
        ]
        frames.extend(args)

        await self.socket.send_multipart(frames)

    async def __send_response(self, *, request_id, args):
        frames = [
            b'',
            b'response',
            request_id,
        ]
        frames.extend(args)

        await self.socket.send_multipart(frames)
