"""
A service class.
"""

from .common import (
    deserialize,
    serialize,
)
from .async_object import AsyncObject
from .client import Client
from .security import (
    generate_hash,
    generate_salt,
)


class ServiceClient(Client):
    def __init__(self, service, **kwargs):
        super().__init__(**kwargs)
        self.service = service

    async def _on_request(self, frames):
        """
        Called whenever a request is received.

        :param frames: The request frames.
        :returns: A list of frames that constitute the reply.
        """
        return await self.service._on_request(frames)


class Service(AsyncObject):
    def __init__(self, socket, shared_secret, name=None, **kwargs):
        super().__init__(**kwargs)

        if name is not None:
            self.name = name

        # If no name was specified, we assume there is one defined at the class
        # level.
        assert self.name, "No service name was specified."

        self._client = ServiceClient(
            service=self,
            socket=socket,
            domain=(b'service', self.name),
            loop=self.loop,
        )
        self.add_cleanup(self._client.close)
        self.add_cleanup(self._client.wait_closed)
        credentials = self.get_credentials(name, shared_secret)
        self.add_task(self._client.register(credentials))

    @staticmethod
    def get_credentials(name, shared_secret):
        salt = generate_salt()
        hash = generate_hash(shared_secret, salt, name)

        return (salt, hash)

    async def _on_request(self, frames):
        print(frames)
        return [b'okay']
