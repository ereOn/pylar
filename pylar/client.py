"""
A client class.
"""


import asyncio

from azmq.common import AsyncTaskObject
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
                request_id = reply.pop(0)
            except IndexError:
                pass
            else:
                future = self._pending_requests.get(request_id)

                if future:
                    future.set_result(reply)


    def get_request_id(self):
        return ('%s' % next(self._request_id_generator)).encode('utf-8')

    async def _request(self, command, *args):
        request_id = self.get_request_id()

        await self.socket.send_multipart((
            b'',
            request_id,
            command,
        ) + args)
        future = asyncio.Future(loop=self.loop)

        @future.add_done_callback
        def request_done(_):
            self._pending_requests.pop(request_id)

        self._pending_requests[request_id] = future
        reply = await future
        return raise_on_error(command=command, reply=reply)

    async def register(self, service_name):
        return await self._request(
            b'register',
            service_name.encode('utf-8'),
        )

    async def unregister(self, service_name):
        return await self._request(
            b'unregister',
            service_name.encode('utf-8'),
        )
