"""
A broker class.
"""


import asyncio
import azmq
import logging
import json

from azmq.common import (
    AsyncTaskObject,
    AsyncTimeout,
    ClosableAsyncObject,
)
from azmq.multiplexer import Multiplexer
from binascii import hexlify
from collections import deque
from functools import partial

from .errors import (
    CallError,
    make_error_frames,
)
from .log import logger as main_logger

logger = main_logger.getChild('broker')


class Client(ClosableAsyncObject):
    def __init__(self, *, socket, identity, timeout, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self.identity = identity
        self._timeout = AsyncTimeout(
            timeout=timeout,
            callback=self.close,
            loop=self.loop,
        )
        self._pending_tasks = set()
        self._services = set()

    def __str__(self):
        return '%x-%s' % (
            id(self.socket),
            hexlify(self.identity).decode('utf-8'),
        )

    def enqueue(self, coro):
        task = asyncio.ensure_future(coro, loop=self.loop)
        task.add_done_callback(self._pending_tasks.remove)
        self._pending_tasks.add(task)

    def refresh(self):
        self._timeout.revive()

    @property
    def services(self):
        return self._services

    def register_service(self, service_name):
        self._services.add(service_name)

    def unregister_service(self, service_name):
        self._services.remove(service_name)

    async def on_close(self):
        tasks = list(self._pending_tasks)

        if tasks:
            logger.warning(
                "Force-cancelling %d task(s) for client disconnection.",
                len(tasks),
            )
            for task in tasks:
                task.cancel()

            await asyncio.gather(
                *tasks,
                return_exceptions=True,
                loop=self.loop,
            )

        self._timeout.close()
        await self._timeout.wait_closed()
        await super().on_close()


class Broker(AsyncTaskObject):
    def __init__(self, *, context, sockets, **kwargs):
        super().__init__(**kwargs)
        self.context = context
        self.context.register_child(self)
        self._multiplexer = Multiplexer(loop=self.loop)

        for socket in sockets:
            self._multiplexer.add_socket(socket)

        self._client_timeout = 5.0
        self._clients = {}
        self._command_handlers = {
            b'register': self._register,
            b'unregister': self._unregister,
            b'call': self._call,
        }
        self._services = {}

    async def on_close(self):
        clients = list(self._clients.values())

        if clients:
            logger.warning(
                "Force-disconnecting %d client(s) for shut-down.",
                len(clients),
            )

            for client in clients:
                client.close()

            await asyncio.gather(
                *[
                    client.wait_closed()
                    for client in clients
                ],
                return_exceptions=True,
                loop=self.loop,
            )

        await super().on_close()

    async def on_run(self):
        while True:
            pairs = await self._multiplexer.recv_multipart()

            for socket, frames in pairs:
                identity = frames.pop(0)
                frames.pop(0)

                client = self._refresh(socket=socket, identity=identity)

                try:
                    type_ = frames.pop(0)
                except IndexError:
                    continue

                if type_ == b'broker':
                    client.enqueue(
                        self._process_request(
                            client=client,
                            frames=frames,
                        ),
                    )
                elif type_ == b'service':
                    client.enqueue(
                        self._process_response(
                            client=client,
                            frames=frames,
                        ),
                    )

    def _refresh(self, *, socket, identity):
        client = self._clients.get((socket, identity))

        if client:
            client.refresh()
        else:
            client = self._connect_client(socket=socket, identity=identity)

        return client

    def _connect_client(self, *, socket, identity):
        client = Client(
            socket=socket,
            identity=identity,
            timeout=self._client_timeout,
            loop=self.loop,
        )
        client.on_closed.connect(
            lambda _: self._disconnect_client(
                socket=socket,
                identity=identity,
            ),
        )
        self._clients[(socket, identity)] = client
        logger.debug("Client %s connected.", client)

        return client

    def _disconnect_client(self, *, socket, identity):
        client = self._clients.pop((socket, identity))
        logger.debug("Client %s disconnected.", client)

        for service_name in client.services:
            self._unregister_service_client(service_name, client)

        return client

    async def _process_request(self, client, frames):
        try:
            request_id = frames.pop(0)
            command = frames.pop(0)
        except IndexError:
            return

        handler = self._command_handlers.get(command)

        if handler:
            try:
                reply = await handler(
                    client,
                    request_id,
                    command,
                    frames,
                ) or []

                await client.socket.send_multipart(
                    [
                        client.identity,
                        b'',
                        b'broker',
                        request_id,
                        b'200',
                    ] + reply,
                )
            except CallError as ex:
                await client.socket.send_multipart(
                    [
                        client.identity,
                        b'',
                        b'broker',
                        request_id,
                    ] + make_error_frames(
                        code=ex.code,
                        message=ex.message,
                    ),
                )
            except asyncio.CancelledError:
                await client.socket.send_multipart(
                    [
                        client.identity,
                        b'',
                        b'broker',
                        request_id,
                    ] + make_error_frames(
                        code=503,
                        message="Request was cancelled.",
                    ),
                )
            except Exception as ex:
                logger.exception(
                    "Unexpected error while handling request %s from %s.",
                    hexlify(request_id),
                    client,
                )
                await client.socket.send_multipart(
                    [
                        client.identity,
                        b'',
                        b'broker',
                        request_id,
                    ] + make_error_frames(
                        code=500,
                        message="Internal error.",
                    ),
                )
        else:
            await client.socket.send_multipart(
                [
                    client.identity,
                    b'',
                    b'broker',
                    request_id,
                ] + make_error_frames(
                    code=404,
                    message="Unknown command.",
                ),
            )

    async def _process_response(self, client, frames):
        try:
            request_id = frames.pop(0)
            command = frames.pop(0)
        except IndexError:
            return

    def _register_service_client(self, service_name, client):
        clients = self._services.get(service_name)

        if not clients:
            self._services[service_name] = deque([client])
            logger.info(
                "New service available: %s.",
                service_name.decode('utf-8'),
            )
        else:
            if client not in clients:
                clients.append(client)

        logger.debug(
            "Service %s registered for client %s.",
            service_name.decode('utf-8'),
            client,
        )

    def _unregister_service_client(self, service_name, client):
        clients = self._services.get(service_name)

        if clients:
            try:
                clients.remove(client)
            except ValueError:
                pass
            else:
                logger.debug(
                    "Service %s unregistered for client %s.",
                    service_name.decode('utf-8'),
                    client,
                )

                if not clients:
                    logger.info(
                        "Service became unavailable: %s.",
                        service_name.decode('utf-8'),
                    )
                    del self._services[service_name]

    async def _register(
        self,
        client,
        request_id,
        command,
        frames,
    ):
        service_name = frames.pop(0)
        self._register_service_client(service_name, client)
        client.register_service(service_name)

    async def _unregister(
        self,
        client,
        request_id,
        command,
        frames,
    ):
        service_name = frames.pop(0)
        client.unregister_service(service_name)
        self._unregister_service_client(service_name, client)

    async def _call(
        self,
        client,
        request_id,
        command,
        frames,
    ):
        service_name = frames.pop(0)
        service_clients = self._services.get(service_name)

        if not service_clients:
            raise CallError(
                code=404,
                message="No such service.",
            )

        service_client = service_clients[0]
        service_clients.rotate(-1)

        await service_client.socket.send_multipart([
            service_client.identity,
            b'',
            b'service',
            client.identity,
            request_id,
        ] + frames)

        result = json.dumps(None, separators=(',', ':')).encode('utf-8')
        return [result]
