"""
Entry-points.
"""

import asyncio
import azmq
import chromalog
import click
import logging
import signal
import sys

from azmq import Context
from contextlib import contextmanager

from .broker import Broker


def setup_logging():
    chromalog.basicConfig(
        level=logging.DEBUG,
        format='[%(levelname)s] %(message)s',
    )
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    logging.getLogger('azmq').setLevel(logging.WARNING)


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


DEFAULT_ENDPOINT = 'tcp://127.0.0.1:3333'


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
@click.argument('endpoints', nargs=-1, metavar='endpoint...')
def broker(endpoints):
    setup_logging()

    if not endpoints:
        endpoints = [
            DEFAULT_ENDPOINT,
        ]

    loop = set_event_loop()
    context = Context(loop=loop)
    sockets = []

    for endpoint in endpoints:
        socket = context.socket(azmq.ROUTER)
        socket.bind(endpoint)
        sockets.append(socket)

    broker = Broker(context=context, sockets=sockets, loop=loop)

    click.echo("Broker started on: %s." % ', '.join(endpoints))

    with allow_interruption(
        (loop, broker.close),
    ):
        loop.run_until_complete(broker.wait_closed())

    context.close()
    loop.run_until_complete(context.wait_closed())

    click.echo("Broker stopped.")


@click.command()
@click.argument('endpoint', default=DEFAULT_ENDPOINT)
def service(endpoint):
    loop = set_event_loop()
    context = Context(loop=loop)

    click.echo("Service started connected to: %s." % endpoint)

    with allow_interruption(
        (loop, context.close),
    ):
        loop.run_until_complete(context.wait_closed())

    click.echo("Service stopped.")
