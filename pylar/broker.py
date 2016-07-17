"""
A broker class.
"""


import asyncio
import azmq
import logging

from azmq.common import (
    AsyncTaskObject,
    AsyncTimeout,
    ClosableAsyncObject,
)
from azmq.multiplexer import Multiplexer
from binascii import hexlify
from functools import partial

from .errors import (
    CallError,
    make_error_frames,
)
from .log import logger as main_logger

logger = main_logger.getChild('broker')


class Client(object):
    def __init__(self, socket, identity):
        self.socket = socket
        self.identity = identity

    def __str__(self):
        return '%x-%s' % (
            id(self.socket),
            hexlify(self.identity).decode('utf-8'),
        )

    def __hash__(self):
        return hash((id(self.socket), self.identity))

    def __eq__(self, other):
        return self.socket == other.socket and self.identity == other.identity

    def __ne__(self, other):
        return not self == other


class ClientInfo(ClosableAsyncObject):
    def __init__(self, *, timeout, **kwargs):
        super().__init__(**kwargs)
        self._timeout = AsyncTimeout(
            timeout=timeout,
            callback=self.close,
            loop=self.loop,
        )
        self._pending_tasks = set()

    def enqueue(self, coro):
        task = asyncio.ensure_future(coro, loop=self.loop)
        task.add_done_callback(self._pending_tasks.remove)
        self._pending_tasks.add(task)

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
        self._client_infos = {}
        self._command_handlers = {
            b'register': self._register,
            b'unregister': self._unregister,
        }

    async def on_close(self):
        client_infos = list(self._client_infos.values())

        if client_infos:
            logger.warning(
                "Force-disconnecting %d client(s) for shut-down.",
                len(client_infos),
            )

            for client_info in client_infos:
                client_info.close()

            await asyncio.gather(
                *[
                    client_info.wait_closed()
                    for client_info in client_infos
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

                client = Client(socket=socket, identity=identity)
                client_info = self._refresh(client)
                client_info.enqueue(
                    self._process_message(
                        client=client,
                        frames=frames,
                    ),
                )

    def _refresh(self, client):
        client_info = self._client_infos.get(client)

        if client_info:
            client_info.refresh()
        else:
            client_info = self._connect_client(client)

        return client_info

    def _connect_client(self, client):
        client_info = ClientInfo(
            timeout=self._client_timeout,
            loop=self.loop,
        )
        client_info.on_closed.connect(
            lambda _: self._disconnect_client(client),
        )
        self._client_infos[client] = client_info
        logger.debug("Client %s connected.", client)

        return client_info

    def _disconnect_client(self, client):
        client_info = self._client_infos.pop(client)
        logger.debug("Client %s disconnected.", client)

        return client_info

    async def _process_message(self, client, frames):
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
                        request_id,
                        b'200',
                    ] + reply,
                )
            except CallError as ex:
                await client.socket.send_multipart(
                    [
                        client.identity,
                        b'',
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
                    request_id,
                ] + make_error_frames(
                    code=404,
                    message="Unknown command.",
                ),
            )

    async def _register(self, client, request_id, command, frames):
        pass

    async def _unregister(self, client, request_id, command, frames):
        pass
