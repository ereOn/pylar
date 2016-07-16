"""
Entry-points.
"""

import asyncio
import click
import signal
import sys

from contextlib import contextmanager

from .broker import Broker


if sys.platform == 'win32':
    from asyncio import ProactorEventLoop as LoopClass
else:
    try:
        from uvloop import EventLoop as LoopClass
    except ImportError:
        from asyncio import SelectorEventLoop as LoopClass


def set_event_loop():
    loop = LoopClass()
    asyncio.set_event_loop(loop)
    return loop


@contextmanager
def allow_interruption(*callbacks):
    if sys.platform == 'win32':
        from ctypes import WINFUNCTYPE, windll
        from ctypes.wintypes import BOOL, DWORD
        kernel32 = windll.LoadLibrary('kernel32')
        phandler_routine = WINFUNCTYPE(BOOL, DWORD)
        setconsolectrlhandler = kernel32.SetConsoleCtrlHandler
        setconsolectrlhandler.argtypes = (phandler_routine, BOOL)
        setconsolectrlhandler.restype = BOOL

        @phandler_routine
        def shutdown(event):
            if event == 0:
                for loop, cb in callbacks:
                    loop.call_soon_threadsafe(cb)

                return 1

            return 0

        if setconsolectrlhandler(shutdown, 1) == 0:
            raise WindowsError()
    else:
        def handler(*args):
            for loop, cb in callbacks:
                loop.call_soon_threadsafe(cb)

        signal.signal(signal.SIGINT, handler)

    try:
        yield
    finally:
        if sys.platform == 'win32':
            if setconsolectrlhandler(shutdown, 0) == 0:
                raise WindowsError()
        else:
            signal.signal(signal.SIGINT, signal.SIG_DFL)


@click.command()
def broker():
    loop = set_event_loop()
    broker = Broker(loop=loop)

    click.echo("Broker started.")

    with allow_interruption(
        (loop, broker.close),
    ):
        loop.run_until_complete(broker.wait_closed())

    click.echo("Broker stopped.")
