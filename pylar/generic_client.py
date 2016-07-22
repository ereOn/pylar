"""
A generic client class.
"""

import asyncio

from binascii import hexlify
from itertools import count
from functools import partial

from .async_object import AsyncObject
from .errors import (
    InvalidReplyError,
    CallError,
)
from .log import logger as main_logger

logger = main_logger.getChild('generic_client')


class GenericClient(AsyncObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # Private members.
        self.__ping_interval = 3
        self.__request_id_generator = count()
        self.__pending_requests = {}

        # Make sure we cancel all pending requests upon closure.
        self.add_cleanup(self.cancel_pending_requests)

        # Call the receiving loop from the entire instance duration.
        self.add_task(self.__receiving_loop())
        self.add_task(self.__heartbeat_loop())

    def cancel_pending_requests(self):
        """
        Cancel all pending requests.
        """
        for future in list(self.__pending_requests.values()):
            future.cancel()

    # Protected methods.

    async def _read(self):
        """
        Read frames.

        :returns: The read frames.

        Must be reimplemented by child classes.
        """
        raise NotImplementedError

    async def _write(self, frames):
        """
        Write frames.

        :param frames: The frames to write.

        Must be reimplemented by child classes.
        """
        raise NotImplementedError

    async def _request(self, frames):
        """
        Send a request and wait for the result.

        :params frames: The frames to send.
        :returns: The request results.
        """
        request_id = self.__request_id()

        await self.__send_request(request_id, frames)

        future = asyncio.Future(loop=self.loop)
        future.add_done_callback(
            partial(self.__remove_request, request_id=request_id),
        )
        self.__pending_requests[request_id] = future

        return await future

    async def _on_request(self, frames):
        """
        Called whenever a request is received.

        :param frames: The request frames.
        :returns: A list of frames that constitute the reply.

        Must be reimplemented by child classes.
        """
        raise NotImplementedError

    async def _notification(self, frames):
        """
        Send a notification.

        :params frames: The frames to send.
        """
        request_id = self.__request_id()

        await self.__send_notification(request_id, frames)

    async def _on_notification(self, frames):
        """
        Called whenever a notification is received.

        :param frames: The request frames.

        Must be reimplemented by child classes.
        """
        raise NotImplementedError

    async def _ping(self):
        """
        Send a ping over the connection.
        """
        request_id = self.__request_id()

        await self.__send_ping(request_id)

    # Private methods.

    def __set_request_result(self, request_id, frames):
        future = self.__pending_requests.get(request_id)
        assert future, "No such active request: %s" % request_id
        assert not future.done(), (
            "Request %s's result was set already." % request_id
        )
        future.set_result(frames)

    def __set_request_exception(self, request_id, frames):
        future = self.__pending_requests.get(request_id)
        assert future, "No such active request: %s" % request_id
        assert not future.done(), (
            "Request %s's result was set already." % request_id
        )
        future.set_exception(frames)

    def __request_id(self):
        return ('%s' % next(self.__request_id_generator)).encode('utf-8')

    def __remove_request(self, future, *, request_id):
        f = self.__pending_requests.pop(request_id)
        assert f is future

    async def __receiving_loop(self):
        while not self.closing:
            frames = await self._read()

            try:
                type_ = frames.pop(0)
                request_id = frames.pop(0)
            except IndexError:
                continue

            if type_ == b'request':
                self.add_task(self.__process_request(request_id, frames))
            elif type_ == b'response':
                self.add_task(self.__process_response(request_id, frames))
            elif type_ == b'notification':
                self.add_task(self.__process_notification(request_id, frames))
            elif type_ == b'ping':
                await self.__send_pong(request_id)
            elif type_ == b'pong':
                pass

    async def __heartbeat_loop(self):
        while not self.closing:
            await asyncio.sleep(self.__ping_interval)
            await self._ping()

    async def __process_request(self, request_id, frames):
        try:
            response = await self._on_request(frames)
        except asyncio.CancelledError:
            await self.__send_error_response(
                request_id,
                408,
                "Request timed out.",
            )
        except CallError as ex:
            await self.__send_error_response(
                request_id,
                ex.code,
                ex.message,
            )
        except Exception as ex:
            logger.exception(
                "Unexpected error while handling request %s.",
                hexlify(request_id),
            )
            await self.__send_error_response(
                request_id,
                500,
                "Internal error.",
            )
        else:
            await self.__send_response(request_id, response or [])

    async def __process_response(self, request_id, frames):
        try:
            code = int(frames.pop(0))
        except (IndexError, ValueError):
            self.__set_request_exception(
                request_id,
                InvalidReplyError(),
            )

        if code == 200:
            self.__set_request_result(request_id, frames)
        else:
            try:
                message = frames.pop(0).decode('utf-8')
            except (IndexError, UnicodeDecodeError):
                self.__set_request_exception(
                    request_id,
                    InvalidReplyError(),
                )
            else:
                self.__set_request_exception(
                    request_id,
                    CallError(
                        code=code,
                        message=message,
                    ),
                )

    async def __process_notification(self, request_id, frames):
        await self._on_notification(frames)

    async def __send_error_response(self, request_id, code, message):
        await self._write([
            b'response',
            request_id,
            ('%d' % code).encode('utf-8'),
            message.encode('utf-8'),
        ])

    async def __send_response(self, request_id, args):
        frames = [
            b'response',
            request_id,
            b'200',
        ]
        frames.extend(args)

        await self._write(frames)

    async def __send_request(self, request_id, args):
        frames = [
            b'request',
            request_id,
        ]
        frames.extend(args)

        await self._write(frames)

    async def __send_notification(self, request_id, args):
        frames = [
            b'notification',
            request_id,
        ]
        frames.extend(args)

        await self._write(frames)

    async def __send_ping(self, request_id):
        await self._write([b'ping', request_id])

    async def __send_pong(self, request_id):
        await self._write([b'pong', request_id])
