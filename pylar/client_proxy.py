"""
A client proxy class.
"""

import asyncio

from functools import partial
from math import ceil

from .async_object import AsyncObject
from .client_context import ClientContext
from .errors import CallError
from .log import logger as main_logger

from pyslot import Signal

logger = main_logger.getChild('client_proxy')


class ClientProxyMeta(type):
    def __new__(cls, name, bases, attrs):
        commands = {}
        notifications = {}

        for base in bases:
            commands.update(getattr(base, '_commands', {}))
            notifications.update(getattr(base, '_notifications', {}))

        attrs.setdefault('_commands', commands)
        attrs.setdefault('_notifications', notifications)

        for name, field in attrs.items():
            command_attrs = getattr(field, '_pylar_command_attrs', None)

            if command_attrs is not None:
                commands[name] = command_attrs

            notification_attrs = getattr(
                field,
                '_pylar_notification_attrs',
                None,
            )

            if notification_attrs is not None:
                notifications[name] = notification_attrs

        return super().__new__(cls, name, bases, attrs)


class ClientProxy(AsyncObject, metaclass=ClientProxyMeta):
    @staticmethod
    def command(use_context=False):
        """
        Register a method as a command handler.

        :param use_context: A boolean flag that indicates whether the specified
            command expects a context as its first unnamed parameter.
        """
        def decorator(func):
            func._pylar_command_attrs = dict(
                use_context=use_context,
            )

            return func

        return decorator

    @staticmethod
    def notification_handler(use_context=False):
        """
        Register a method as a notification handler.

        :param use_context: A boolean flag that indicates whether the specified
            notification expects a context as its first unnamed parameter.
        """
        def decorator(func):
            func._pylar_notification_attrs = dict(
                use_context=use_context,
            )

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

        self.client.register_client_proxy(self)
        self.add_cleanup(partial(self.client.unregister_client_proxy, self))

    @property
    def token(self):
        return self.__token

    @token.setter
    def token(self, value):
        was_registered = self.registered
        self.__token = value

        if value is None:
            self.__unregistered.set()
            self.__registered.clear()

            if was_registered:
                logger.info(
                    "Client is no longer registered as %s.",
                    self.context,
                )
                self.on_unregistered.emit(self)
        else:
            self.__unregistered.clear()
            self.__registered.set()

            if not was_registered:
                logger.info("Client is now registered as %s.", self.context)
                self.on_registered.emit(self)

    @property
    def context(self):
        return ClientContext(domain=self.domain, token=self.token)

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

    async def request(self, target_domain, command, args=()):
        await self.wait_registered()

        client_proxy = self.client.get_client_proxy(target_domain)

        # If we have a local client proxy that matches, we don't need to
        # contact the broker about it and can make the request locally.
        if client_proxy:
            return await client_proxy.on_request(
                source_domain=self.domain,
                source_token=self.token,
                command=command,
                args=args,
            )
        else:
            return await self.client.request(
                source_domain=self.domain,
                target_domain=target_domain,
                command=command,
                args=args,
            )

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
        :param command: The command, as a string.
        :param args: The additional frames.
        :returns: The result.
        """
        command_attrs = self._commands.get(command)

        if command_attrs is None:
            raise CallError(
                code=404,
                message="Unknown command: %s." % command,
            )

        command = getattr(self, command)
        command_args = []

        if command_attrs['use_context']:
            context = ClientContext(
                domain=source_domain,
                token=source_token,
            )
            command_args.append(context)

        command_args.extend(args)

        return await command(*command_args)

    async def notification(self, target_domain, type_, args=()):
        await self.wait_registered()

        client_proxy = self.client.get_client_proxy(target_domain)

        # If we have a local client proxy that matches, we don't need to
        # contact the broker about it and can make the request locally.
        if client_proxy:
            return await client_proxy.on_notification(
                source_domain=self.domain,
                source_token=self.token,
                type_=type_,
                args=args,
            )
        else:
            return await self.client.notification(
                source_domain=self.domain,
                target_domain=target_domain,
                type_=type_,
                args=args,
            )

    async def on_notification(
        self,
        source_domain,
        source_token,
        type_,
        args,
    ):
        """
        Called whenever a notification is received.

        :param source_domain: The caller's domain.
        :param source_token: The caller's token.
        :param type_: The type, as a string.
        :param args: The additional frames.
        """
        notification_attrs = self._notifications.get(type_)
        context = ClientContext(
            domain=source_domain,
            token=source_token,
        )

        if notification_attrs is None:
            logger.warning(
                "Ignoring unknown notification '%s' from %s.",
                type_,
                context,
            )
            return

        notification = getattr(self, type_)
        notification_args = []

        if notification_attrs['use_context']:
            notification_args.append(context)

        notification_args.extend(args)

        await notification(*notification_args)

    async def query(self, target_domain):
        """
        Query the broker for a given domain.

        :param target_domain: The target domain to query.
        """
        await self.wait_registered()

        return await self.client.query(
            source_domain=self.domain,
            target_domain=target_domain,
        )

    async def transmit(self, target_domain, x_domain, x_token, frames):
        """
        Transmit a message to the broken on behalf of another domain.

        :param target_domain: The target domain to transmit the message to.
        :param x_domain: The impersonated domain.
        :param x_token: The impersonated domain's token.
        :param frames: The frames.
        """
        await self.wait_registered()

        return await self.client.transmit(
            source_domain=self.domain,
            target_domain=target_domain,
            x_domain=x_domain,
            x_token=x_token,
            frames=frames,
        )

    async def notification_transmit(
        self,
        target_domain,
        type_,
        x_domain,
        x_token,
        frames,
    ):
        """
        Send a notification on behalf of another domain.
        """
        return await self.client.notification_transmit(
            source_domain=self.domain,
            target_domain=target_domain,
            type_=type_,
            x_domain=x_domain,
            x_token=x_token,
            frames=frames,
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
                logger.debug(
                    "Registration for %s in progress...",
                    self.context,
                )
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
