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

    def __init__(self, *, socket, domain, credentials=None, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self.domain = domain
        self.credentials = credentials
        self._token = None
        self._registered = asyncio.Event(loop=self.loop)
        self._unregistered = asyncio.Event(loop=self.loop)
        self._unregistered.set()
        self.__registration_timeout = 5.0
        self.__ping_timeout = 5.0
        self.__ping_interval = 5.0
        self.add_task(self.__register_loop())

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, value):
        if value is None:
            self._registered.clear()
            self._unregistered.set()
            self._token = None
        else:
            self._registered.set()
            self._unregistered.clear()
            self._token = value

    @property
    def registered(self):
        return self._registered.is_set()

    async def wait_registered(self):
        """
        Wait for the client instance to be registered.
        """
        return await self._registered.wait()

    async def register(self):
        """
        Register on the broker.
        """
        assert self.credentials, "Can't register without credentials."

        frames = [b'register']
        frames.extend(self.domain)
        frames.append(b'')
        frames.extend(self.credentials)

        self.token = tuple(await self._request(frames))
        logger.info("Client is now registered.")

    async def wait_unregistered(self):
        """
        Wait for the client instance to be unregistered.
        """
        return await self._unregistered.wait()

    async def unregister(self):
        """
        Unregister from the broker.
        """
        await self._request([b'unregister'])

        self.token = None
        logger.info("Client is no longer registered.")

    async def call(self, domain, args):
        """
        Send a generic call to a specified domain.

        :param domain: The target domain.
        :param args: A list of frames to pass.
        :returns: The call results.
        """
        frames = [b'call']
        frames.extend(domain)
        frames.append(b'')
        frames.extend(args)

        return await self._request(frames)

    async def ping(self):
        """
        Ping the broker.
        """
        before = perf_counter()
        await self._request([b'ping'])
        return perf_counter() - before

    async def describe(self):
        result = await self.call((b'service', b'rpc'), [b'describe'])
        return deserialize(result[0])

    async def method_call(self, domain, method, args=None, kwargs=None):
        """
        Remote call to a specified domain.

        :param domain: The target domain.
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

        result = await self.call(domain, frames)
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

    async def _on_request(self, frames):
        """
        Called whenever a request is received.

        :param frames: The request frames.
        :returns: A list of frames that constitute the reply.
        """
        sep_index = frames.index(b'')
        domain = tuple(frames[:sep_index])
        del frames[:sep_index + 1]
        sep_index = frames.index(b'')
        token = tuple(frames[:sep_index])
        del frames[:sep_index + 1]
        command = frames.pop(0)
        return await self.on_call(domain, token, command, frames)

    async def _on_notification(self, frames):
        """
        Called whenever a notification is received.

        :param frames: The request frames.
        """
        type_ = frames.pop(0)

        await self.on_notification(type_, frames)

    async def on_call(self, domain, token, command, args):
        """
        Called whenever a call is received.

        :param domain: The caller's domain.
        :param token: The caller's token.
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

        return await command_handler(self, domain, token, args)

    async def on_notification(self, type_, args):
        """
        Called whenever a notification is received.

        :param type_: The type of the notification.
        :param args: The arguments.
        """
        logger.warning("Received unhandled notification of type %s.", type_)

    async def __register_loop(self):
        min_delay = 1
        max_delay = 60
        factor = 1.5
        delay = 1

        while not self.closing:
            if self.registered:
                try:
                    await asyncio.wait_for(self.ping(), self.__ping_timeout)
                except asyncio.CancelledError:
                    raise
                except asyncio.TimeoutError:
                    logger.warning(
                        "Broker did not reply in %s second(s). Performing "
                        "implicit unregistration.",
                        self.__ping_timeout,
                    )
                    self.token = None
                except Exception as ex:
                    logger.error(
                        "Ping request failed (%s). Performing implicit "
                        "unregistration.",
                        ex,
                    )
                    self.token = None

                await asyncio.sleep(self.__ping_interval, loop=self.loop)
            else:
                try:
                    logger.debug("Registration in progress...")
                    await asyncio.wait_for(
                        self.register(),
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
