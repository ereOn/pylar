"""
A client class.
"""

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
        self.token = None

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

    async def unregister(self):
        """
        Unregister from the broker.
        """
        await self._request([b'unregister'])

        self.token = None

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

        if type_ == b'registration_required':
            await self.on_registration_required()
        else:
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

    async def on_registration_required(self):
        """
        Called whenever registration is required.
        """
        if self.credentials is None:
            logger.warning(
                "Received registration request but no credentials were "
                "specified.",
            )
        else:
            logger.debug("Received registration request: registering.")
            await self.register()
