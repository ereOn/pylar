"""
A RPC client proxy class.
"""

from .client_proxy import ClientProxy
from .common import (
    deserialize,
    serialize,
)
from .log import logger as main_logger
from .rpc import deserialize_function

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

    async def get_rpc_service_proxy(self, target_domain):
        """
        Get a RPC service proxy.

        :param target_domain: The domain of the service to get a proxy for.
        :returns: A RPC service proxy.
        """
        desc = await self.describe(target_domain)

        class ServiceProxyMeta(type):
            @staticmethod
            def make_method(name, signature, documentation):
                async def method(self, *args, **kwargs):
                    bound_arguments = signature.bind(*args, **kwargs)

                    return await self._client_proxy.method_call(
                        target_domain=self._domain,
                        method=name,
                        args=bound_arguments.args,
                        kwargs=bound_arguments.kwargs,
                    )

                method.__doc__ = documentation

                return method

            def __new__(cls, name, bases, attrs):
                description = attrs.pop('description')

                for name, method_desc in description['methods'].items():
                    signature, documentation = deserialize_function(
                        method_desc,
                    )
                    attrs[name] = cls.make_method(
                        name=name,
                        signature=signature,
                        documentation=documentation,
                    )

                return super().__new__(cls, name, bases, attrs)

        class ServiceProxy(object, metaclass=ServiceProxyMeta):
            description = desc

            def __init__(self, domain, client_proxy):
                self._domain = domain
                self._client_proxy = client_proxy

        return ServiceProxy(
            domain=target_domain,
            client_proxy=self,
        )
