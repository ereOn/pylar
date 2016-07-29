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
from .rpc import serialize_function
from .service import Service

logger = main_logger.getChild('rpc_service')


class MethodAttributes(dict):
    def __init__(self, **kwargs):
        kwargs.setdefault('use_context', False)
        super().__init__(**kwargs)


class RPCServiceMeta(ClientProxyMeta):
    EXPOSED_METHODS_DECORATED = 'decorated'
    EXPOSED_METHODS_PUBLIC = 'public'
    EXPOSED_METHODS_VALUES = (
        EXPOSED_METHODS_DECORATED,
        EXPOSED_METHODS_PUBLIC,
    )

    def __new__(cls, name, bases, attrs):
        methods = {}

        for base in bases:
            methods.update(getattr(base, '_methods', {}))

        attrs.setdefault('_methods', methods)

        exposed_methods = attrs.get(
            'exposed_methods',
            cls.EXPOSED_METHODS_DECORATED,
        )

        assert exposed_methods in cls.EXPOSED_METHODS_VALUES, (
            "Unknown `exposed_methods` attribute value %r. Must be one of "
            "%s" % (
                exposed_methods,
                ', '.join(map(repr, cls.EXPOSED_METHODS_VALUES)),
            )
        )

        for name, field in attrs.items():
            method_attrs = getattr(field, '_pylar_method_attrs', None)

            if method_attrs is None and \
                exposed_methods == cls.EXPOSED_METHODS_PUBLIC and \
                not name.startswith('_') and \
                callable(field):
                    method_attrs = MethodAttributes()

            if method_attrs is not None:
                methods[name] = method_attrs

        return super().__new__(cls, name, bases, attrs)


class RPCService(Service, metaclass=RPCServiceMeta):
    @staticmethod
    def method(use_context=False):
        """
        Register a method as a method handler.

        :param use_context: A boolean flag that indicates whether the specified
            method expects a context as its first unnamed parameter.
        """
        def decorator(func):
            func._pylar_method_attrs = MethodAttributes(
                use_context=use_context,
            )

            return func

        return decorator

    @Service.command()
    async def describe(self):
        description = {
            'methods': {
                method_name: serialize_function(
                    getattr(self, method_name),
                    use_context=method_attrs['use_context'],
                )
                for method_name, method_attrs in self._methods.items()
            },
        }

        return [serialize(description)]

    @Service.command(use_context=True)
    async def method_call(
        self,
        context,
        method_name,
        method_args,
        method_kwargs,
    ):
        method_name = method_name.decode('utf-8')
        method_args = deserialize(method_args)
        method_kwargs = deserialize(method_kwargs)
        method_attrs = self._methods.get(method_name)

        if method_attrs is None:
            raise CallError(
                code=404,
                message="No such method.",
            )

        method = getattr(self, method_name)

        if method_attrs['use_context']:
            method_args.insert(0, context)

        result = await method(
            *method_args,
            **method_kwargs
        )

        return [serialize(result)]
