"""
A client class.
"""

from .common import (
    deserialize,
    serialize,
)
from .generic_client import GenericClient


class Client(GenericClient):
    def __init__(self, *, socket, domain, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self.domain = domain
        self.token = None

    async def register(self, credentials):
        """
        Register on the broker.

        :param credentials: The credentials to use for registration.
        """
        frames = [b'register']
        frames.extend(self.domain)
        frames.append(b'')
        frames.extend(credentials)

        self.token = await self._request(frames)

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
        return [b'47']
