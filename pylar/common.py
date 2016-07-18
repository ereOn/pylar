"""
Common class and utilities.
"""

import asyncio

from itertools import count


class Requester(object):
    """
    Implements generic uniquely-identified requests.
    """

    def __init__(self, *, loop):
        self.__loop = loop
        self.__request_id_generator = count()
        self.__pending_requests = {}

    async def send_request(self, request_id, **kwargs):
        """
        Effectively sends the request.

        Must be defined by inheriting classes.
        """
        raise NotImplementedError

    async def request(self, **kwargs):
        """
        Send a request and wait for the result.

        :returns: The request results.
        """
        request_id = self.__request_id()
        await self.send_request(request_id, **kwargs)
        future = asyncio.Future(loop=self.__loop)
        future.add_done_callback(
            partial(self.__remove_request, request_id=request_id),
        )
        self.__pending_requests[request_id] = future
        return await future

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
