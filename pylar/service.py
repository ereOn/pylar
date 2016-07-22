"""
A service class.
"""

import asyncio

from .client import Client
from .log import logger as main_logger
from .security import (
    generate_hash,
    generate_salt,
)

logger = main_logger.getChild('service')


class Service(Client):
    def __init__(self, shared_secret, name=None, **kwargs):
        if name is not None:
            self.name = name

        # If no name was specified, we assume there is one defined at the class
        # level.
        assert self.name, "No service name was specified."

        self.shared_secret = shared_secret

        super().__init__(
            domain=(b'service', self.name.encode('utf-8')),
            credentials=self.get_credentials(
                self.name.encode('utf-8'),
                self.shared_secret,
            ),
            **kwargs,
        )

    @staticmethod
    def get_credentials(name, shared_secret):
        salt = generate_salt()
        hash = generate_hash(shared_secret, salt, name)

        return (salt, hash)
