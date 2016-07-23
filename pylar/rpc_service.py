"""
RPC service.
"""

import inspect

from .common import (
    deserialize,
    serialize,
)
from .errors import CallError
from .log import logger as main_logger
from .service import Service

logger = main_logger.getChild('authentication_service')


class RPCService(Service):
    name = 'rpc'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # TODO: Pass model from the arguments.
        class Model(object):
            def add(self, x, y): pass
            def remove(self, x, y): pass


        model = Model()
        self._model = model

    @Service.command('describe')
    async def _describe(self, domain, token, args):
        result = {
            'methods': {},
            'contants': {},
        }

        return [serialize(result)]
