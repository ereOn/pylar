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
    async def dispatch(self, context, target_domain, *frames):
        """
        Transmit a message from one broker to another.

        :param context: The caller's context.
        :param target_domain: The target domain.
        :param frames: The frames.
        """
        service = await self.iservice.get_service_for(
            target_domain=target_domain,
            ignore_services=[self],
        )

        if not service:
            raise CallError(
                code=404,
                message="No such domain: %s." % target_domain,
            )

        return await service.transmit(
            target_domain=target_domain,
            x_domain=context.domain,
            x_token=context.token,
            frames=frames,
        )

    @Service.notification_handler(use_context=True)
    async def notification_dispatch(
        self,
        context,
        type_,
        target_domain,
        *frames
    ):
        service = await self.iservice.get_service_for(
            target_domain=target_domain,
            ignore_services=[self],
        )

        if not service:
            logger.warning(
                "Dropping notification '%s' from %s to unknown service %s.",
                type_.decode('utf-8'),
                context,
                target_domain.decode('utf-8'),
            )
            return

        await service.notification_transmit(
            target_domain=target_domain,
            type_=type_,
            x_domain=context.domain,
            x_token=context.token,
            frames=frames,
        )
