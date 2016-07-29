"""
A RPC client proxy class.
"""

from .client_proxy import ClientProxy
from .common import (
    deserialize,
    serialize,
)
from .log import logger as main_logger

logger = main_logger.getChild('rpc_client_proxy')


class RPCClientProxy(ClientProxy):
    async def describe(self, target_domain):
        """
        Ask a remote service to describe its available methods.

        :param target_domain: The target domain of the remote service.
        """
        result = await self.request(
            target_domain=target_domain,
            command='describe',
        )

        return deserialize(result[0])

    async def method_call(
        self,
        target_domain,
        method,
        args=None,
        kwargs=None,
    ):
        """
        Remote call to a specified domain.

        :param domain: The domain.
        :param target_domain: The target domain.
        :param method: The method to call.
        :param args: A list of arguments to pass.
        :param kwargs: A list of named arguments to pass.
        :returns: The method call results.
        """
        result = await self.request(
            target_domain=target_domain,
            command='method_call',
            args=[
                method.encode('utf-8'),
                serialize(list(args) or []),
                serialize(dict(kwargs or {})),
            ],
        )

        return deserialize(result[0])
