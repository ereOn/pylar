"""
A service class.
"""

import struct

from io import BytesIO

from .rpc_client_proxy import RPCClientProxy
from .log import logger as main_logger
from .security import (
    generate_hash,
    generate_salt,
)

logger = main_logger.getChild('service')


class Service(RPCClientProxy):
    def __init__(self, *, shared_secret, **kwargs):
        super().__init__(
            domain=b'service/%s' % self.name.encode('utf-8'),
            credentials=self.get_credentials(
                self.name.encode('utf-8'),
                shared_secret,
            ),
            **kwargs
        )
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
