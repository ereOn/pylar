"""
A client class.
"""

import asyncio

from math import ceil
from time import perf_counter

from .common import (
    deserialize,
    serialize,
)
from .errors import CallError
from .generic_client import GenericClient
from .log import logger as main_logger

logger = main_logger.getChild('client')


class ClientMeta(type):
    def __new__(cls, name, bases, attrs):
        command_handlers = attrs.setdefault('_command_handlers', {})

        for field in attrs.values():
            command_name = getattr(field, '_pylar_command_name', None)

            if command_name is not None:
                command_handlers[command_name] = field

        return type.__new__(cls, name, bases, attrs)


class ClientProxy(object):
    def __init__(self, client, domain, credentials):
        self.client = client
        self.domain = domain
        self.credentials = credentials
        self.task = self.client.add_task(self.__register_loop()),

        self.__registration_timeout = 5.0
        self.__registered = asyncio.Event(loop=client.loop)
        self.__unregistered = asyncio.Event(loop=client.loop)

        self.token = None

    @property
    def token(self):
        return self.__token

    @token.setter
    def token(self, value):
        self.__token = value

        if value is None:
            self.__unregistered.set()
            self.__registered.clear()
        else:
            self.__unregistered.clear()
            self.__registered.set()

    async def wait_registered(self):
        await self.__registered.wait()

    async def wait_unregistered(self):
        await self.__unregistered.wait()

    async def on_request(
        self,
        source_domain,
        source_token,
        command,
        args,
    ):
        """
        Called whenever a request is received.

        :param source_domain: The caller's domain.
        :param source_token: The caller's token.
        :param command: The command.
        :param args: The additional frames.
        :returns: The result.
        """
        command_handler = self._command_handlers.get(command)

        if not command_handler:
            raise CallError(
                code=404,
                message="Unknown command.",
            )

        return await command_handler(
            self,
            domain,
            source_domain,
            source_token,
            args,
        )

    async def __register_loop(self):
        min_delay = 1
        max_delay = 60
        factor = 1.5
        delay = 1

        while not self.client.closing:
            await self.wait_unregistered()

            try:
                logger.debug("Registration for %s in progress...", self.domain)
                self.token = await asyncio.wait_for(
                    self.client._register(
                        domain=self.domain,
                        credentials=self.credentials,
                    ),
                    self.__registration_timeout,
                )
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                logger.warning(
                    "Registration did not complete within %s second(s). "
                    "Retrying in %s second(s).",
                    self.__registration_timeout,
                    delay,
                )
                await asyncio.sleep(delay, loop=self.client.loop)
                delay = min(ceil(delay * factor), max_delay)
            except Exception as ex:
                logger.error(
                    "Registration failed (%s): retrying in %s second(s).",
                    ex,
                    delay,
                )
                await asyncio.sleep(delay, loop=self.client.loop)
                delay = min(ceil(delay * factor), max_delay)
            else:
                delay = min_delay


class Client(GenericClient, metaclass=ClientMeta):
    @staticmethod
    def command(name=None):
        """
        Register a method as a command handler.

        :param name: The name of the command to register.
        """
        def decorator(func):
            func._pylar_command_name = (name or func.__name__).encode('utf-8')

            return func

        return decorator

    def __init__(self, *, socket, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self._token = None
        self._registered = asyncio.Event(loop=self.loop)
        self._unregistered = asyncio.Event(loop=self.loop)
        self._unregistered.set()
        self.__ping_timeout = 5.0
        self.__ping_interval = 5.0
        self.__has_registrations = asyncio.Event(loop=self.loop)
        self.__client_proxies = {}
        self.add_task(self.__ping_loop())

    def add_registration(self, domain, credentials):
        assert domain not in self.__client_proxies

        self.__client_proxies[domain] = registration = ClientProxy(
            client=self,
            domain=domain,
            credentials=credentials,
        )
        self.__has_registrations.set()

        return registration

    async def describe(self, domain):
        result = await self._request(domain, (b'service', b'rpc'), [b'describe'])
        return deserialize(result[0])

    async def method_call(
        self,
        domain,
        target_domain,
        method,
        args=None,
        kwargs=None,
    ):
        """
        Remote call to a specified domain.

        :param domain: The domain.
        :param target_domain: The target domain.
        :param method: The method to call.
        :param args: A list of arguments to pass.
        :param kwargs: A list of named arguments to pass.
        :returns: The method call results.
        """
        frames = [
            b'method_call',
            method.encode('utf-8'),
            serialize(list(args) or []),
            serialize(dict(kwargs or {})),
        ]

        result = await self._request(domain, target_domain, frames)
        return deserialize(result[0])

    # Protected methods.

    async def _read(self):
        """
        Read frames.

        :returns: The read frames.
        """
        frames = await self.socket.recv_multipart()
        frames.pop(0)  # Empty frame.

        return frames

    async def _write(self, frames):
        """
        Write frames.

        :param frames: The frames to write.
        """
        frames.insert(0, b'')
        await self.socket.send_multipart(frames)

    async def _register(self, domain, credentials):
        """
        Register on the broker.

        :param domain: The domain to register for.
        :param credentials: The credentials for the domain.
        :returns: The authentication token.
        """
        frames = [b'register', domain, credentials]
        token, = await super()._request(frames)
        logger.info("Client is now registered as %s.", domain)

        return token

    async def _unregister(self, domain):
        """
        Unregister from the broker.
        """
        frames = [b'unregister', domain]

        await super()._request(frames)

        logger.info("Client is no longer registered as %s.", domain)

    async def _request(self, domain, target_domain, args):
        """
        Send a generic request to a specified domain.

        :param domain: The domain.
        :param target_domain: The target domain.
        :param args: A list of frames to pass.
        :returns: The request result.
        """
        frames = [b'request', domain, target_domain]
        frames.extend(args)

        return await super()._request(frames)

    async def _ping(self):
        """
        Ping the broker.
        """
        before = perf_counter()
        await super()._request([b'ping'])
        return perf_counter() - before

    async def _on_request(self, frames):
        """
        Called whenever a request is received.

        :param frames: The request frames.
        :returns: A list of frames that constitute the reply.
        """
        domain = frames.pop(0)
        registration = self.__client_proxies.get(domain)

        if not registration:
            raise CallError(
                code=404,
                message="Client not found.",
            )

        source_domain = frames.pop(0)
        source_token = frames.pop(0)
        command = frames.pop(0)

        return await registration.on_request(
            source_domain,
            source_token,
            command,
            frames,
        )

    async def _on_notification(self, frames):
        """
        Called whenever a notification is received.

        :param frames: The request frames.
        """
        type_ = frames.pop(0)

        await self.on_notification(type_, frames)

    async def on_notification(self, type_, args):
        """
        Called whenever a notification is received.

        :param type_: The type of the notification.
        :param args: The arguments.
        """
        logger.warning("Received unhandled notification of type %s.", type_)

    async def __ping_loop(self):
        while not self.closing:
            await self.__has_registrations.wait()

            try:
                await asyncio.wait_for(self._ping(), self.__ping_timeout)
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                logger.warning(
                    "Broker did not reply in %s second(s). Performing "
                    "implicit unregistration.",
                    self.__ping_timeout,
                )

                # Flush the outgoing queues.
                await self.socket.reset_all()

                for client_proxy in self.__client_proxies.values():
                    client_proxy.token = None

            except Exception as ex:
                logger.error(
                    "Ping request failed (%s). Performing implicit "
                    "unregistration.",
                    ex,
                )

                # Flush the outgoing queues.
                await self.socket.reset_all()

                for client_proxy in self.__client_proxies.values():
                    client_proxy.token = None

            await asyncio.sleep(self.__ping_interval, loop=self.loop)
