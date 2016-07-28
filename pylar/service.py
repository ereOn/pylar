"""
A service class.
"""

import asyncio
import struct

from io import BytesIO

from .client_proxy import ClientProxy
from .log import logger as main_logger
from .security import (
    generate_hash,
    generate_salt,
)

logger = main_logger.getChild('service')


class Service(ClientProxy):
    def __init__(self, *, shared_secret, name, **kwargs):
        super().__init__(
            domain=b'service/%s' % name.encode('utf-8'),
            credentials=self.get_credentials(
                name.encode('utf-8'),
                shared_secret,
            ),
            **kwargs
        )
        self.name = name
        self.shared_secret = shared_secret

    @staticmethod
    def get_credentials(name, shared_secret):
        salt = generate_salt()
        hash = generate_hash(shared_secret, salt, name)

        buf = BytesIO()
        buf.write(struct.pack('B', len(salt)))
        buf.write(salt)
        buf.write(hash)
        return buf.getvalue()
