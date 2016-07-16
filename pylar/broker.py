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
from azmq.containers import AsyncList
from chromalog.mark.helpers.simple import warning as important
from collections import namedtuple

from .log import logger as main_logger

logger = main_logger.getChild('broker')


class Client(object):
    def __init__(self, identity, socket, timeout):
        self.identity = identity
        self.socket = socket
        self.timeout = timeout
        self.service_name = None


class Service(object):
    def __init__(self, service_name, loop):
        self.service_name = service_name
        self.clients = AsyncList(loop=loop)

    def register_client(self, client):
        if client not in self.clients:
            self.clients.append(client)
            logger.info(
                "Registered service %s for client %s.",
                important(self.service_name.decode('utf-8')),
                client.identity,
            )

    def unregister_client(self, client):
        if client in self.clients:
            self.clients.remove(client)
            logger.info(
                "Unregistered service %s for client %s.",
                important(self.service_name.decode('utf-8')),
                client.identity,
            )


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
        self._pending_tasks = set()
        self._services = {}

    async def on_close(self):
        for task in self._pending_tasks:
            task.cancel()
            await task

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
                    def register_client(socket, identity):
                        async def disconnect_client():
                            await self.disconnect_client(identity)

                        self._clients[identity] = client = Client(
                            identity=identity,
                            socket=socket,
                            timeout=AsyncTimeout(
                                callback=disconnect_client,
                                timeout=self._client_timeout,
                                loop=self.loop,
                            ),
                        )
                        logger.debug("Registered client: %s.", identity)

                        return client

                    client = register_client(socket=socket, identity=identity)
                else:
                    client.socket = socket
                    client.timeout.revive()

                task = asyncio.ensure_future(
                    self.handle_client_message(
                        client=client,
                        identity=identity,
                        msg=frames,
                    ),
                    loop=self.loop,
                )
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.remove)

    async def disconnect_client(self, identity):
        client = self._clients.pop(identity, None)

        if client:
            if client.service_name is not None:
                service = self._services.get(client.service_name)

                if service:
                    service.unregister_client(client)

            logger.debug("Unregistered client: %s.", identity)

        return client

    async def handle_client_message(self, client, identity, msg):
        command = msg.pop(0)

        if command == b'register':
            client.service_name = msg.pop(0)
            service = self._services.get(client.service_name)

            if not service:
                self._services[client.service_name] = service = Service(
                    service_name=client.service_name,
                    loop=self.loop,
                )

            service.register_client(client)

        elif command == b'unregister':
            service_name = msg.pop(0)
            client.service_name = None
            service = self._services.get(client.service_name)

            if service:
                service.unregister_client(client)
