"""
Arithmetic example service.
"""

from .log import logger as main_logger
from .rpc_service import RPCService

logger = main_logger.getChild('arithmetic_service')


class ArithmeticService(RPCService):
    def __init__(self, **kwargs):
        super().__init__(
            name='arithmetic',
            **kwargs
        )

    @RPCService.method()
    async def sum(self, *values):
        return sum(values)
