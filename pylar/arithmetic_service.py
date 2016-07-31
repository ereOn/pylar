"""
Arithmetic example service.
"""

from .log import logger as main_logger
from .rpc_service import RPCService

logger = main_logger.getChild('arithmetic_service')


class ArithmeticService(RPCService):
    exposed_methods = 'public'

    def __init__(self, **kwargs):
        super().__init__(
            name='arithmetic',
            **kwargs
        )

    def sum(self, *values):
        """
        Get the sum of the specified values.

        :param values: Values to sum up.
        :returns: The sum.
        """
        return sum(values)
