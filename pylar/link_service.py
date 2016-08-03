"""
Link service.
"""

from .domain import user_domain
from .errors import CallError
from .log import logger as main_logger
from .service import Service

logger = main_logger.getChild('authentication_service')


class LinkService(Service):
    name = 'link'

    def __init__(self, *, iservice, **kwargs):
        super().__init__(**kwargs)
        self.iservice = iservice

    @Service.command(use_context=True)
    async def transmit(self, context, *frames):
        """
        Transmit a message from one broker to another.

        :param context: The caller's context.
        :param frames: The frames.
        """
        return [b'']
