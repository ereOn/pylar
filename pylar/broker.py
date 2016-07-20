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
from itertools import (
    chain,
    count,
)

from .common import Requester
from .errors import (
    CallError,
    raise_on_error,
)
from .log import logger as main_logger

logger = main_logger.getChild('broker')


class Connection(ClosableAsyncObject):
    def __init__(self, *, socket, identity, timeout, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self.identity = identity
        self.domain = None
        self._timeout = AsyncTimeout(
            timeout=timeout,
            callback=self.close,
            loop=self.loop,
        )
        self._pending_tasks = set()
        self._requester = Requester(
            loop=self.loop,
            send_request_callback=self.__send_request,
        )

    def __str__(self):
        return '%x-%s' % (
            id(self.socket),
            hexlify(self.identity).decode('utf-8'),
        )

    async def on_close(self):
        self._requester.cancel_pending_requests()
        tasks = list(self._pending_tasks)

        if tasks:
            logger.warning(
                "Force-cancelling %d task(s) after disconnection.",
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

    def enqueue(self, coro):
        task = asyncio.ensure_future(coro, loop=self.loop)
        task.add_done_callback(self._pending_tasks.remove)
        self._pending_tasks.add(task)

    def refresh(self):
        self._timeout.revive()

    async def request(self, *args):
        result = await self._requester.request(args=chain(*args))
        return raise_on_error(result)

    async def response(self, request_id, code, args):
        await self.socket.send_multipart([
            self.identity,
            b'',
            b'response',
            request_id,
            ('%d' % code).encode('utf-8'),
        ] + list(args))

    async def error_response(self, request_id, code, message):
        await self.response(
            request_id,
            code,
            [message.encode('utf-8')],
        )

    async def __send_request(self, *, request_id, args):
        await self.socket.send_multipart([
            self.identity,
            b'',
            b'request',
            request_id,
        ] + list(args))


class Broker(AsyncTaskObject):
    def __init__(self, *, context, sockets, **kwargs):
        super().__init__(**kwargs)
        self.context = context
        self.context.register_child(self)
        self._multiplexer = Multiplexer(loop=self.loop)

        for socket in sockets:
            self._multiplexer.add_socket(socket)

        self._connection_timeout = 5.0
        self._connections = {}
        self._connections_by_domain = {}
        self._command_handlers = {
            b'register': self._register,
            b'unregister': self._unregister,
            b'call': self._call,
        }

    async def on_close(self):
        connections = list(self._connections.values())

        if connections:
            logger.warning(
                "Force-disconnecting %d connection(s) for shut-down.",
                len(connections),
            )

            for connection in connections:
                connection.close()

            await asyncio.gather(
                *[
                    connection.wait_closed()
                    for connection in connections
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
                frames.pop(0)  # Empty frame.

                connection = self._refresh_connection(
                    socket=socket,
                    identity=identity,
                )

                try:
                    type_ = frames.pop(0)
                    request_id = frames.pop(0)
                except IndexError:
                    continue

                if type_ == b'request':
                    connection.enqueue(
                        self._process_request(request_id, connection, frames),
                    )
                elif type_ == b'response':
                    connection._requester.set_request_result(
                        request_id,
                        frames,
                    )

    async def _process_request(self, request_id, connection, frames):
        try:
            command = frames.pop(0)
        except IndexError:
            return

        handler = self._command_handlers.get(command)

        if handler:
            try:
                reply = await handler(connection, frames) or []

                await connection.response(request_id, 200, reply)
            except CallError as ex:
                await connection.error_response(
                    request_id,
                    ex.code,
                    ex.message,
                )
            except asyncio.CancelledError:
                await connection.error_response(
                    request_id,
                    503,
                    "Request was cancelled.",
                )
            except Exception as ex:
                logger.exception(
                    "Unexpected error while handling request %s from %s.",
                    hexlify(request_id),
                    connection,
                )
                await connection.error_response(
                    request_id,
                    500,
                    "Internal error.",
                )
        else:
            await connection.error_response(
                request_id,
                404,
                "Unknown command.",
            )

    async def _register(self, connection, frames):
        if connection.domain:
            raise CallError(
                code=412,
                message="Already registered.",
            )

        sep_index = frames.index(b'')
        domain = tuple(frames[:sep_index])
        credentials = tuple(frames[sep_index + 1:])
        self._register_connection(connection, domain)

    async def _unregister(self, connection, frames):
        if not connection.domain:
            raise CallError(
                code=412,
                message="Not registered.",
            )

        self._unregister_connection(connection)

    async def _call(self, connection, frames):
        sep_index = frames.index(b'')
        domain = tuple(frames[:sep_index])
        connections = self._connections_by_domain.get(domain)

        if not connections:
            raise CallError(
                code=404,
                message="No such domain: %r." % (domain,),
            )

        frames = frames[sep_index + 1:]
        connection = connections[0]
        connections.rotate(-1)

        return await connection.request(
            (b'call',),
            frames,
        )

    # Private methods.

    def _refresh_connection(self, *, socket, identity):
        connection = self._connections.get((socket, identity))

        if connection:
            connection.refresh()
        else:
            connection = self._add_connection(socket=socket, identity=identity)

        return connection

    def _add_connection(self, *, socket, identity):
        connection = Connection(
            socket=socket,
            identity=identity,
            timeout=self._connection_timeout,
            loop=self.loop,
        )
        connection.on_closed.connect(self._remove_connection)
        self._connections[(socket, identity)] = connection
        logger.debug("Connection with %s established.", connection)

        return connection

    def _remove_connection(self, connection):
        if connection.domain:
            self._unregister_connection(connection)

        del self._connections[(connection.socket, connection.identity)]
        logger.debug("Connection with %s removed.", connection)

        return connection

    def _register_connection(self, connection, domain):
        connections = self._connections_by_domain.setdefault(domain, deque())

        if not connections:
            logger.info("Domain %s is now available.", domain)

        connections.append(connection)
        connection.domain = domain
        logger.debug(
            "Registered domain %s for connection %s.",
            domain,
            connection,
        )

    def _unregister_connection(self, connection):
        connections = self._connections_by_domain[connection.domain]
        connections.remove(connection)

        logger.info(
            "Unregistered domain %s for connection %s.",
            connection.domain,
            connection,
        )

        if not connections:
            del self._connections_by_domain[connection.domain]
            logger.info("Domain %s is now unavailable.", connection.domain)

        connection.domain = None
