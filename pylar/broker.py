"""
A broker class.
"""


import asyncio
import azmq
import logging

from azmq.common import (
    AsyncTaskObject,
    AsyncTimeout,
    cancel_on_closing,
)
from azmq.multiplexer import Multiplexer
from collections import namedtuple

from .log import logger as main_logger

logger = main_logger.getChild('broker')


Client = namedtuple('_Client', ('socket', 'timeout'))


class Broker(AsyncTaskObject):
    def __init__(self, context, endpoints, **kwargs):
        super().__init__(**kwargs)
        self.context = context
        self.context.register_child(self)
        self._multiplexer = Multiplexer(loop=self.loop)

        for endpoint in endpoints:
            self._multiplexer.add_socket(self.create_socket(endpoint))

        self._clients = {}
        self._client_timeout = 5.0

    async def on_close(self):
        if self._clients:
            logger.warning(
                "Force-closing %s remaining client(s).",
                len(self._clients),
            )

            for identity in list(self._clients):
                client = await self.disconnect_client(identity)
                client.timeout.close()
                await client.timeout.wait_closed()

        await super().on_close()

    def create_socket(self, endpoint):
        socket = self.context.socket(azmq.ROUTER)
        socket.bind(endpoint)
        return socket

    async def on_run(self):
        while True:
            pairs = await self._multiplexer.recv_multipart()

            for socket, frames in pairs:
                identity = frames.pop(0)
                frames.pop(0)
                client = self._clients.get(identity)

                if not client:
                    async def disconnect_client():
                        await self.disconnect_client(identity)

                    self._clients[identity] = client = Client(
                        socket=socket,
                        timeout=AsyncTimeout(
                            callback=disconnect_client,
                            timeout=self._client_timeout,
                            loop=self.loop,
                        ),
                    )
                    logger.debug("Registered client: %s.", identity)
                else:
                    client.socket = socket
                    client.timeout.revive()

    async def disconnect_client(self, identity):
        client = self._clients.pop(identity, None)

        if client:
            logger.debug("Unregistered client: %s.", identity)

        return client
