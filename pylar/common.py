"""
Common class and utilities.
"""

import asyncio
import json

from itertools import count
from functools import partial


def serialize(value):
    """
    Serialize a value for sending over the network.

    :param value: The value to serialize.
    :returns: Bytes.
    """
    return json.dumps(value, separators=(',', ':')).encode('utf-8')


def deserialize(value):
    """
    Deserialize a value read on the network.

    :param value: The value to deserialize.
    :returns: The value.
    """
    return json.loads(value.decode('utf-8'))


class Requester(object):
    """
    Implements generic uniquely-identified requests.
    """

    def __init__(self, *, loop, send_request_callback):
        self.__loop = loop
        self.__send_request_callback = send_request_callback
        self.__request_id_generator = count()
        self.__pending_requests = {}

    async def request(self, *args, **kwargs):
        """
        Send a request and wait for the result.

        :returns: The request results.
        """
        request_id = self.__request_id()

        await self.__send_request_callback(
            *args,
            request_id=request_id,
            **kwargs
        )
        future = asyncio.Future(loop=self.__loop)
        future.add_done_callback(
            partial(self.__remove_request, request_id=request_id),
        )
        self.__pending_requests[request_id] = future
        return await future

    def cancel_pending_requests(self):
        """
        Cancel all pending requests.
        """
        for future in list(self.__pending_requests.values()):
            future.cancel()

    def cancel_request(self, request_id):
        """
        Cancel the specified request.

        :param request_id: The request to cancel.
        """
        future = self.__pending_requests.get(request_id)
        assert future, "No such active request: %s" % request_id
        assert not future.done(), (
            "Request %s's result was set already." % request_id
        )
        future.cancel()

    def set_request_result(self, request_id, result):
        """
        Set the result for the specified request and unblocks the associated
        pending `request` call.

        :param request_id: The request for which the result must be set. This
            request must be pending and have no result or exception set
            already.
        :param result: The result to assign to the request.
        """
        future = self.__pending_requests.get(request_id)
        assert future, "No such active request: %s" % request_id
        assert not future.done(), (
            "Request %s's result was set already." % request_id
        )
        future.set_result(result)

    def set_request_exception(self, request_id, exception):
        """
        Set the exception for the specified request and unblocks the associated
        pending `request` call.

        :param request_id: The request for which the exception must be set.
            This request must be pending and have no result or exception set
            already.
        :param exception: The exception to assign to the request.
        """
        future = self.__pending_requests.get(request_id)
        assert future, "No such active request: %s" % request_id
        assert not future.done(), (
            "Request %s's result was set already." % request_id
        )
        future.set_exception(exception)

    # Private methods below.

    def __request_id(self):
        return ('%s' % next(self.__request_id_generator)).encode('utf-8')

    def __remove_request(self, future, *, request_id):
        f = self.__pending_requests.pop(request_id)
        assert f is future
