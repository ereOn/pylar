"""
A broker class.
"""


import asyncio
import azmq
import logging

from azmq.common import AsyncTimeout
from binascii import hexlify
from collections import deque
from functools import partial

from .async_object import AsyncObject
from .errors import CallError
from .generic_client import GenericClient
from .log import logger as main_logger
from .security import verify_hash

logger = main_logger.getChild('broker')


class Connection(GenericClient):
    def __init__(self, *, socket, identity, on_request_cb, timeout, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self.identity = identity

        self.__on_request_cb = on_request_cb

        # The receiving queue.
        self.__queue = asyncio.Queue(loop=self.loop)

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
        self.token = None

    def __str__(self):
        return hexlify(self.identity).decode('utf-8')

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

    async def _read(self):
        """
        Read frames.

        :returns: The read frames.
        """
        return await self.__queue.get()

    async def _write(self, frames):
        """
        Write frames.

        :param frames: The frames to write.
        """
        frames.insert(0, b'')
        frames.insert(0, self.identity)
        await self.socket.send_multipart(frames)

    async def _on_request(self, frames):
        """
        Called whenever a request is received.

        :param frames: The request frames.
        :returns: A list of frames that constitute the reply.
        """
        return await self.__on_request_cb(self, frames)

class Broker(AsyncObject):
    SERVICE_AUTHENTICATION_DOMAIN = (b'service', b'authentication')

    def __init__(self, *, context, socket, shared_secret, **kwargs):
        super().__init__(**kwargs)
        self.context = context
        self.socket = socket
        self.shared_secret = shared_secret

        self.__connection_timeout = 5.0
        self.__connections = {}
        self.__connections_by_domain = {}
        self.__command_handlers = {
            b'register': self.__register_request,
            b'unregister': self.__unregister_request,
            b'call': self.__call_request,
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

    # Private methods.

    def __refresh_connection(self, identity):
        connection = self.__connections.get(identity)

        if connection:
            connection.refresh()
        else:
            connection = self.__add_connection(identity)

        return connection

    def __add_connection(self, identity):
        connection = Connection(
            socket=self.socket,
            identity=identity,
            on_request_cb=self.__process_request,
            timeout=self.__connection_timeout,
            loop=self.loop,
        )
        connection.add_cleanup(partial(self.__remove_connection, connection))
        self.__connections[identity] = connection
        logger.debug("Connection with %s established.", connection)

        return connection

    def __remove_connection(self, connection):
        if connection.domain:
            self.__unregister_connection(connection)

        del self.__connections[connection.identity]
        logger.debug("Connection with %s removed.", connection)

        return connection

    def __register_connection(self, connection, domain, token):
        connections = self.__connections_by_domain.setdefault(domain, deque())

        if not connections:
            logger.info("Domain %s is now available.", domain)

        connections.append(connection)
        connection.domain = domain
        connection.token = token
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
            frames = await self.socket.recv_multipart()
            identity = frames.pop(0)
            frames.pop(0)  # Empty frame.

            connection = self.__refresh_connection(identity)

            await connection.receive(frames)

    async def __process_request(self, connection, frames):
        command = frames.pop(0)
        handler = self.__command_handlers.get(command)

        if not handler:
            raise CallError(code=400, message="Bad request.")

        return await handler(connection, frames)

    async def __register_request(self, connection, frames):
        if connection.domain:
            raise CallError(
                code=412,
                message="Already registered.",
            )

        sep_index = frames.index(b'')
        domain = tuple(frames[:sep_index])
        credentials = tuple(frames[sep_index + 1:])

        # As a special rule for the authentication server, we check the
        # credentials manually.
        if domain == self.SERVICE_AUTHENTICATION_DOMAIN:
            if not self.__verify_authentication_credentials(credentials):
                raise CallError(
                    code=401,
                    message="Invalid shared secret.",
                )

            token = ()
        else:
            connections = self.__connections_by_domain.get(
                self.SERVICE_AUTHENTICATION_DOMAIN,
            )

            if not connections:
                raise CallError(
                    code=503,
                    message="Authentication service unavailable.",
                )

            auth_connection = connections[0]
            connections.rotate(-1)
            auth_frames = [b'authenticate']
            auth_frames.extend(frames)

            token = await auth_connection._request(auth_frames)

        self.__register_connection(connection, domain, token)

        return token

    async def __unregister_request(self, connection, frames):
        if not connection.domain:
            raise CallError(
                code=412,
                message="Not registered.",
            )

        self.__unregister_connection(connection)

    async def __call_request(self, connection, frames):
        sep_index = frames.index(b'')
        domain = tuple(frames[:sep_index])
        connections = self.__connections_by_domain.get(domain)

        if not connections:
            raise CallError(
                code=404,
                message="No such domain: %r." % (domain,),
            )

        frames = frames[sep_index + 1:]
        frames.insert(0, b'call')
        connection = connections[0]
        connections.rotate(-1)

        return await connection._request(frames)

    def __verify_authentication_credentials(self, credentials):
        salt, hash = credentials

        return verify_hash(self.shared_secret, salt, b'authentication', hash)
