"""
A client proxy class.
"""

import asyncio

from math import ceil

from .async_object import AsyncObject
from .common import (
    deserialize,
    serialize,
)
from .errors import CallError
from .log import logger as main_logger

from pyslot import Signal

logger = main_logger.getChild('client_proxy')


class ClientProxyMeta(type):
    def __new__(cls, name, bases, attrs):
        command_handlers = attrs.setdefault('_command_handlers', {})

        for field in attrs.values():
            command_name = getattr(field, '_pylar_command_name', None)

            if command_name is not None:
                command_handlers[command_name] = field

        return type.__new__(cls, name, bases, attrs)


class ClientProxy(AsyncObject, metaclass=ClientProxyMeta):
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

    def __init__(self, *, client, domain, credentials, **kwargs):
        super().__init__(**kwargs)
        self.client = client
        self.domain = domain
        self.credentials = credentials
        self.task = self.add_task(self.__register_loop()),

        # Exposed signals.
        self.on_registered = Signal()
        self.on_unregistered = Signal()

        self.__registration_timeout = 5.0
        self.__registered = asyncio.Event(loop=client.loop)
        self.__unregistered = asyncio.Event(loop=client.loop)

        self.__token = None
        self.token = None

    @property
    def token(self):
        return self.__token

    @token.setter
    def token(self, value):
        if value is None:
            self.__unregistered.set()
            self.__registered.clear()

            if self.__token is not None:
                logger.info(
                    "Client is no longer registered as %s.",
                    self.domain,
                )
                self.on_unregistered.emit(self)
        else:
            self.__unregistered.clear()
            self.__registered.set()

            if self.__token is None:
                logger.info("Client is now registered as %s.", self.domain)
                self.on_registered.emit(self)

        self.__token = value

    @property
    def registered(self):
        return self.__registered.is_set()

    @property
    def unregistered(self):
        return self.__unregistered.is_set()

    async def wait_registered(self):
        await self.__registered.wait()

    async def wait_unregistered(self):
        await self.__unregistered.wait()

    # TODO: Move.
    async def describe(self, domain):
        result = await self._request(
            domain,
            (b'service', b'rpc'),
            [b'describe'],
        )
        return deserialize(result[0])

    # TODO: Move.
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
            source_domain,
            source_token,
            args,
        )

    async def __register_loop(self):
        min_delay = 1
        max_delay = 60
        factor = 1.5
        delay = 1

        while not self.closing:
            await self.wait_unregistered()
            await self.client.wait_connection()

            try:
                logger.debug("Registration for %s in progress...", self.domain)
                self.token = await asyncio.wait_for(
                    self.client._register(
                        domain=self.domain,
                        credentials=self.credentials,
                    ),
                    self.__registration_timeout,
                    loop=self.loop,
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
                await asyncio.sleep(delay, loop=self.loop)
                delay = min(ceil(delay * factor), max_delay)
            except Exception as ex:
                logger.error(
                    "Registration failed (%s): retrying in %s second(s).",
                    ex,
                    delay,
                )
                await asyncio.sleep(delay, loop=self.loop)
                delay = min(ceil(delay * factor), max_delay)
            else:
                delay = min_delay
