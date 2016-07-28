"""
A service class.
"""

import struct

from .common import (
    deserialize,
    serialize,
)
from .errors import CallError
from .log import logger as main_logger
from .client_proxy import ClientProxyMeta
from .service import Service

logger = main_logger.getChild('rpc_service')


class RPCServiceMeta(ClientProxyMeta):
    def __new__(cls, name, bases, attrs):
        method_handlers = {}

        for base in bases:
            method_handlers.update(getattr(base, '_method_handlers', {}))

        attrs.setdefault('_method_handlers', method_handlers)

        for field in attrs.values():
            method_name = getattr(field, '_pylar_method_name', None)

            if method_name is not None:
                method_handlers[method_name] = field

        return super().__new__(cls, name, bases, attrs)


class RPCService(Service, metaclass=RPCServiceMeta):
    @staticmethod
    def method(name=None):
        """
        Register a method as a method handler.

        :param name: The name of the method to register.
        """
        def decorator(func):
            func._pylar_method_name = (name or func.__name__).encode('utf-8')

            return func

        return decorator

    @Service.command('describe')
    async def describe(self, source_domain, source_token, args):
        # TODO: Implement.
        return [serialize({})]

    @Service.command('method_call')
    async def method_call(self, source_domain, source_token, args):
        method_name = args.pop(0)
        method_args = deserialize(args.pop(0))
        method_kwargs = deserialize(args.pop(0))

        method = self._method_handlers.get(method_name)

        if not method:
            raise CallError(
                code=404,
                message="No such method.",
            )

        result = await method(
            self,
            source_domain,
            source_token,
            *method_args,
            **method_kwargs
        )

        return [serialize(result)]
