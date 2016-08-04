"""
A base inter-service class.
"""

from .async_object import AsyncObject
from .log import logger as main_logger

logger = main_logger.getChild('iservice')


class IService(AsyncObject):
    service_class = None

    def __init__(self, *, clients, shared_secret, **kwargs):
        super().__init__(**kwargs)
        self.clients = clients
        self.shared_secret = shared_secret
        self.services = [
            self.service_class(
                client=client,
                shared_secret=shared_secret,
                iservice=self,
                **kwargs
            )
            for client in self.clients
        ]
