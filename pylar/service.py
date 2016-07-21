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


class Service(Client):
    def __init__(self, shared_secret, name=None, **kwargs):
        if name is not None:
            self.name = name

        # If no name was specified, we assume there is one defined at the class
        # level.
        assert self.name, "No service name was specified."

        super().__init__(
            domain=(b'service', self.name),
            **kwargs,
        )

        credentials = self.get_credentials(name, shared_secret)
        self.add_task(self.register(credentials))

    @staticmethod
    def get_credentials(name, shared_secret):
        salt = generate_salt()
        hash = generate_hash(shared_secret, salt, name)

        return (salt, hash)

    async def _on_request(self, frames):
        return [b'okay']
