"""
A link inter-service class.
"""

import asyncio

from cachetools import TTLCache
from collections import deque
from functools import partial

from .log import logger as main_logger
from .iservice import IService
from .link_service import LinkService

logger = main_logger.getChild('link-iservice')


class LinkIService(IService):
    service_class = LinkService

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__services_by_domain = TTLCache(maxsize=100, ttl=5)

    async def get_service_for(self, target_domain, ignore_services):
        services = self.__services_by_domain.get(target_domain)

        if services:
            services.rotate(-1)
            return services[0]
        else:
            services = []

        event = asyncio.Event(loop=self.loop)

        queries = {
            service: asyncio.ensure_future(
                service.query(target_domain),
                loop=self.loop,
            )
            for service in self.services
            if not service in ignore_services
        }

        def query_done(service, task):
            queries.pop(service)

            if not task.cancelled() and not task.exception():
                services.append(service)

        for service, task in queries.items():
            task.add_done_callback(partial(query_done, service))

        while queries and not services:
            await asyncio.wait(
                queries.values(),
                return_when=asyncio.FIRST_COMPLETED,
                loop=self.loop,
            )

        for task in queries.values():
            task.cancel()

        if services:
            self.__services_by_domain[target_domain] = deque(services)
            return services[0]
