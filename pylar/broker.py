"""
A broker class.
"""


import asyncio
import azmq
import logging

from azmq.common import AsyncTimeout
from azmq.multiplexer import Multiplexer
from binascii import hexlify
from collections import deque
from functools import partial

from .async_object import AsyncObject
from .errors import CallError
from .generic_client import GenericClient
from .log import logger as main_logger

logger = main_logger.getChild('broker')


class Connection(GenericClient):
    def __init__(self, *, socket, identity, timeout, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self.identity = identity

        # The receiving queue.
        self.__queue = asyncio.Queue(loop=self.loop)
        self._read = self.__queue.get

        # The dying timer.
        self.__timeout = AsyncTimeout(
            timeout=timeout,
            callback=self.close,
            loop=self.loop,
        )
        self.add_cleanup(self.__timeout.close)
        self.add_cleanup(self.__timeout.wait_closed)

        # Public attributes.
        self.domain = None

    def __str__(self):
        return '%x-%s' % (
            id(self.socket),
            hexlify(self.identity).decode('utf-8'),
        )

    def refresh(self):
        """
        Resets the instance dying timer.
        """
        self.__timeout.revive()

    async def receive(self, frames):
        """
        Receive frames.

        :param frames: The frames to receive.
        """
        await self.__queue.put(frames)

    async def _write(self, frames):
        """
        Write frames.

        :param frames: The frames to write.
        """
        frames.insert(0, b'')
        frames.insert(0, self.identity)
        await self.socket.send_multipart(frames)

    async def _on_request(self, request_id, frames):
        """
        Called whenever a request is received.

        :param request_id: A unique request id that must be sent back.
        :param frames: The request frames.
        :returns: A list of frames that constitute the reply.
        """
        return [b'42']


class Broker(AsyncObject):
    def __init__(self, *, context, sockets, **kwargs):
        super().__init__(**kwargs)
        self.context = context
        self.__multiplexer = Multiplexer(loop=self.loop)

        for socket in sockets:
            self.__multiplexer.add_socket(socket)

        self.__connection_timeout = 5.0
        self.__connections = {}
        self.__connections_by_domain = {}
        self.__command_handlers = {
            b'register': self._register,
            b'unregister': self._unregister,
            b'call': self._call,
        }

        self.add_cleanup(self.force_disconnections)
        self.add_task(self.__receiving_loop())

    async def force_disconnections(self):
        connections = list(self.__connections.values())

        if connections:
            logger.warning(
                "Force-disconnecting %d connection(s).",
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

    # Protected methods.

    async def _register(self, connection, frames):
        if connection.domain:
            raise CallError(
                code=412,
                message="Already registered.",
            )

        sep_index = frames.index(b'')
        domain = tuple(frames[:sep_index])
        credentials = tuple(frames[sep_index + 1:])
        self.__register_connection(connection, domain)

    async def _unregister(self, connection, frames):
        if not connection.domain:
            raise CallError(
                code=412,
                message="Not registered.",
            )

        self.__unregister_connection(connection)

    async def _call(self, connection, frames):
        sep_index = frames.index(b'')
        domain = tuple(frames[:sep_index])
        connections = self.__connections_by_domain.get(domain)

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

    def __refresh_connection(self, *, socket, identity):
        connection = self.__connections.get((socket, identity))

        if connection:
            connection.refresh()
        else:
            connection = self.__add_connection(socket=socket, identity=identity)

        return connection

    def __add_connection(self, *, socket, identity):
        connection = Connection(
            socket=socket,
            identity=identity,
            timeout=self.__connection_timeout,
            loop=self.loop,
        )
        connection.add_cleanup(partial(self.__remove_connection, connection))
        self.__connections[(socket, identity)] = connection
        logger.debug("Connection with %s established.", connection)

        return connection

    def __remove_connection(self, connection):
        if connection.domain:
            self.__unregister_connection(connection)

        del self.__connections[(connection.socket, connection.identity)]
        logger.debug("Connection with %s removed.", connection)

        return connection

    def __register_connection(self, connection, domain):
        connections = self.__connections_by_domain.setdefault(domain, deque())

        if not connections:
            logger.info("Domain %s is now available.", domain)

        connections.append(connection)
        connection.domain = domain
        logger.debug(
            "Registered domain %s for connection %s.",
            domain,
            connection,
        )

    def __unregister_connection(self, connection):
        connections = self.__connections_by_domain[connection.domain]
        connections.remove(connection)

        logger.info(
            "Unregistered domain %s for connection %s.",
            connection.domain,
            connection,
        )

        if not connections:
            del self.__connections_by_domain[connection.domain]
            logger.info("Domain %s is now unavailable.", connection.domain)

        connection.domain = None

    async def __receiving_loop(self):
        while True:
            pairs = await self.__multiplexer.recv_multipart()

            for socket, frames in pairs:
                identity = frames.pop(0)
                frames.pop(0)  # Empty frame.

                connection = self.__refresh_connection(
                    socket=socket,
                    identity=identity,
                )
                await connection.receive(frames)
