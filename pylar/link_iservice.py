"""
A link inter-service class.
"""

from .log import logger as main_logger
from .iservice import IService
from .link_service import LinkService

logger = main_logger.getChild('link-iservice')


class LinkIService(IService):
    service_class = LinkService
