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
        self.domains = {}

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

    async def request(self, domain, source_domain, source_token, args):
        """
        Send a generic request from a specified domain.

        :param domain: The domain for which the request is destined.
        :param source_domain: The source domain in behalf of which the request
            is made.
        :param source_token: The token for the source domain.
        :param args: A list of frames to pass.
        :returns: The request result.
        """
        assert domain is not None

        frames = list(domain)
        frames.append(b'')
        frames.extend(source_domain or ())
        frames.append(b'')
        frames.extend(token or ())
        frames.append(b'')
        frames.extend(args)

        return await self._request(frames)

    async def _on_request(self, frames):
        """
        Called whenever a request is received.

        :param frames: The request frames.
        :returns: A list of frames that constitute the reply.
        """
        return await self.__on_request_cb(self, frames)

class Broker(AsyncObject):
    SERVICE_DOMAIN_PREFIX = b'service'
    SERVICE_AUTHENTICATION_DOMAIN = (SERVICE_DOMAIN_PREFIX, b'authentication')

    def __init__(self, *, socket, shared_secret, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self.shared_secret = shared_secret

        self.__connection_timeout = 10.0
        self.__connections = {}
        self.__connections_by_domain = {}
        self.__command_handlers = {
            b'register': self.__register_request,
            b'unregister': self.__unregister_request,
            b'request': self.__request_request,
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
        for domain in list(connection.domains):
            self.__unregister_connection(connection, domain)

        del self.__connections[connection.identity]
        logger.debug("Connection with %s removed.", connection)

        return connection

    def __register_connection(self, connection, domain, token):
        connections = self.__connections_by_domain.setdefault(domain, deque())

        if not connections:
            logger.info("Domain %s is now available.", domain)

        connections.append(connection)
        connection.domains[domain] = token
        logger.debug(
            "Registered domain %s for connection %s.",
            domain,
            connection,
        )

    def __unregister_connection(self, connection, domain):
        connections = self.__connections_by_domain[domain]
        connections.remove(connection)

        logger.debug(
            "Unregistered domain %s for connection %s.",
            domain,
            connection,
        )

        if not connections:
            del self.__connections_by_domain[domain]
            logger.info("Domain %s is now unavailable.", domain)

        del connection.domains[domain]

    async def __receiving_loop(self):
        while True:
            frames = await self.socket.recv_multipart()
            identity = frames.pop(0)
            frames.pop(0)  # Empty frame.

            connection = self.__refresh_connection(identity)

            await connection.receive(frames)

    async def __process_request(self, connection, frames):
        command = frames.pop(0)

        if command == b'ping':
            return []

        sep_index = frames.index(b'')
        domain = tuple(frames[:sep_index])
        handler = self.__command_handlers.get(command)

        if not handler:
            raise CallError(code=400, message="Bad request.")

        return await handler(connection, domain, frames[sep_index + 1:])

    async def __register_request(self, connection, domain, frames):
        credentials = tuple(frames)

        # Services are authenticated via a shared secret.
        if domain[0] == self.SERVICE_DOMAIN_PREFIX:
            if not self.__verify_service_credentials(domain[1], credentials):
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
                logger.warning(
                    "Received authentication request for %s but no "
                    "authentication service is currently available !",
                    domain,
                )
                raise CallError(
                    code=503,
                    message="Authentication service unavailable.",
                )

            auth_connection = connections[0]
            connections.rotate(-1)

            token = await auth_connection.request(
                domain=self.SERVICE_AUTHENTICATION_DOMAIN,
                source_domain=domain,
                source_token=(),
                args=[
                    b'authenticate',
                ] + list(credentials),
            )

        if domain in connection.domains:
            self.__unregister_connection(connection, domain)

        self.__register_connection(connection, domain, token)

        return token

    async def __unregister_request(self, connection, domain, frames):
        self.__unregister_connection(connection, domain)

    async def __request_request(self, connection, domain, frames):
        if domain not in connection.domains:
            raise CallError(
                code=412,
                message="Not registered.",
            )

        sep_index = frames.index(b'')
        target_domain = tuple(frames[:sep_index])
        connections = self.__connections_by_domain.get(target_domain)

        if not connections:
            raise CallError(
                code=404,
                message="No such domain: %r." % (target_domain,),
            )

        target_connection = connections[0]
        connections.rotate(-1)

        return await target_connection.request(
            domain=target_domain,
            source_domain=domain,
            source_token=connection.domains[domain],
            args=frames[sep_index + 1:],
        )

    def __verify_service_credentials(self, service_name, credentials):
        salt, hash = credentials

        return verify_hash(self.shared_secret, salt, service_name, hash)
