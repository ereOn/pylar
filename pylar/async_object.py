"""
A base class for all asyncio-compatible classes.
"""


import asyncio

from functools import wraps


def cancel_on_closing(func):
    """
    Mark a method to be cancelled as soon as the parent is closing.
    """
    assert asyncio.iscoroutinefunction(func), (
        "func must be a coroutine function."
    )

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        return await self.run_until_closing(coro)

    return wrapper


class AsyncObject(object):
    def __init__(self, *, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        self._cleanup_functions = []
        self._closing = asyncio.Event(loop=self.loop)
        self._close_task = asyncio.ensure_future(self._close(), loop=self.loop)

    def add_cleanup(self, func):
        """
        Add a function or coroutine function to call upon closure.

        :param func: A function, coroutine or coroutine function to call upon
            closure. If `func` is a coroutine, it will be awaited for.
        """
        self._cleanup_functions.append(func)

    def remove_cleanup(self, func):
        """
        Remove a function, coroutine or coroutine function registered to be
        called upon closure.

        :param func: A function, coroutine or coroutine function to remove from
            the cleanup.
        """
        self._cleanup_functions.remove(func)

    def add_task(self, coro):
        """
        Executes the specified coro until it completes or the instance is
        closing.

        :param coro: The coroutine to execute.
        :returns: The task.
        """
        task = asyncio.ensure_future(
            self.run_until_closing(coro),
            loop=self.loop,
        )
        task.add_done_callback(self.on_task_done)
        self.add_cleanup(task)

        return task

    def on_task_done(self, task):
        """
        Called whenever a task is done.

        :param task: The task that completed.
        """
        self.remove_cleanup(task)

        if not task.cancelled():
            exception = task.exception()

            if exception:
                self.on_task_exception(task)

    def on_task_exception(self, task):
        """
        Called whenever a task raises an exception.

        :param task: The task that raised an error.
        """
        raise NotImplementedError

    async def run_until_closing(self, coro):
        """
        Await for the specified coroutine until its over or the instance is
        closing.

        :param coro: The coroutine to wait for.
        :returns: The result of the coroutine.
        """
        closing_task = asyncio.ensure_future(
            self.wait_closing(),
            loop=self.loop,
        )
        coro_task = asyncio.ensure_future(coro, loop=self.loop)
        await asyncio.wait(
            [closing_task, coro_task],
            loop=self.loop,
            return_when=asyncio.FIRST_COMPLETED,
        )

        closing_task.cancel()
        coro_task.cancel()

        return await coro_task

    def close(self):
        """
        Close this object.

        Causes all methods decorated by `cancel_on_closing` to be cancelled.
        """
        self._closing.set()

    @property
    def closing(self):
        """
        A boolean flag that indicates whether the instance is closing or
        closed.
        """
        return self._closing.is_set()

    @property
    def closed(self):
        """
        A boolean flag that indicates whether the instance is closed.
        """
        return self._close_task.done()

    async def wait_closing(self):
        """
        Wait for the instance to be closing.
        """
        await self._closing.wait()

    async def wait_closed(self):
        """
        Wait for the instance to be closed.
        """
        await asyncio.shield(self._close_task, loop=self.loop)

    # Special methods.

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.close()
        await self.wait_closed()

    # Private methods.

    async def _close(self):
        await self._closing.wait()

        for func in self._cleanup_functions[:]:
            if asyncio.iscoroutinefunction(func):
                try:
                    await func()
                except asyncio.CancelledError:
                    pass
            elif asyncio.iscoroutine(func) or hasattr(func, '__await__'):
                try:
                    await func
                except asyncio.CancelledError:
                    pass
            else:
                func()
