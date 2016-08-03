"""
A link inter-service class.
"""

import asyncio

from collections import deque

from .log import logger as main_logger
from .iservice import IService
from .link_service import LinkService

logger = main_logger.getChild('link-iservice')


class LinkIService(IService):
    service_class = LinkService

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__services_by_domain = {}

    async def get_service_for(self, target_domain, ignore_services):
        services = self.__services_by_domain.get(target_domain)

        if services:
            # TODO: Make the cache non-permanent.
            services.rotate(-1)
            return services[0]

        event = asyncio.Event(loop=self.loop)

        async def check_domain(service, target_domain):
            try:
                await service.query(target_domain)
                return service
            except Exception:
                logger.exception("Query failed")
                return

        # TODO: Better wait mechanisms in case of timeout, error, succes, ...
        # Basically, we should wait for the first success only except if
        # everything failed.
        done, pending = await asyncio.wait(
            [
                check_domain(service, target_domain)
                for service in self.services
                if service not in ignore_services
            ],
            return_when=asyncio.FIRST_COMPLETED,
            loop=self.loop,
        )

        for task in pending:
            task.cancel()

        if done:
            services = await asyncio.gather(*list(done), loop=self.loop)
            self.__services_by_domain[target_domain] = deque(services)
            return services[0]
