"""
Entry-points.
"""

import asyncio
import azmq
import chromalog
import click
import entrypoints
import importlib
import logging
import signal
import sys
import traceback

from azmq import Context
from base64 import b64decode
from contextlib import contextmanager

from .broker import Broker
from .client import Client


def setup_logging(debug):
    chromalog.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format='[%(levelname)s] %(message)s',
    )

    if debug:
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
DEFAULT_SHARED_SECRET = b'changethissecret'


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


def import_class(dotted_name):
    module_name, class_name = dotted_name.rsplit('.', 1)
    module = importlib.import_module(module_name)

    return getattr(module, class_name)


def import_service(name):
    if '.' in name:
        return import_class(name)
    else:
        return entrypoints.get_single('pylar_services', name).load()


def import_iservice(name):
    if '.' in name:
        return import_class(name)
    else:
        return entrypoints.get_single('pylar_iservices', name).load()


def check_shared_secret(shared_secret):
    if shared_secret is None:
        click.echo(
            click.style(
                "No shared secret was specified ! A default one will be used."
                " Production usage is *NOT* recommended.",
                fg='yellow',
            ),
            err=True,
        )
        return DEFAULT_SHARED_SECRET
    else:
        return b64decode(shared_secret)


@click.command()
@click.option(
    '-d',
    '--debug',
    is_flag=True,
    default=None,
    help="Enabled debug output.",
)
@click.option(
    '-s',
    '--shared-secret',
    default=None,
    help="A shared secret in base64 format that the authentication services "
    "use too.",
)
@click.option('-l', '--listen', nargs=1, metavar='endpoint', multiple=True)
def broker(debug, shared_secret, listen):
    setup_logging(debug=debug)

    shared_secret = check_shared_secret(shared_secret)

    if not listen:
        listen = [
            DEFAULT_ENDPOINT,
        ]

    loop = set_event_loop()
    context = Context(loop=loop)
    socket = context.socket(azmq.ROUTER)

    for endpoint in listen:
        socket.bind(endpoint)

    broker = Broker(
        socket=socket,
        shared_secret=shared_secret,
        loop=loop,
    )

    click.echo("Broker started on %s." % ', '.join(listen))

    with allow_interruption(
        (loop, broker.close),
    ):
        try:
            loop.run_until_complete(broker.wait_closed())
        except Exception as ex:
            click.echo(
                click.style(
                    "Exception while running service: %s" % ex,
                    fg='red',
                ),
                err=True,
            )

    context.close()
    loop.run_until_complete(context.wait_closed())

    click.echo("Broker stopped.")


@click.command()
@click.option(
    '-d',
    '--debug',
    is_flag=True,
    default=None,
    help="Enabled debug output.",
)
@click.option(
    '-s',
    '--shared-secret',
    default=None,
    help="A shared secret in base64 format that the authentication services "
    "use too.",
)
@click.option('-c', '--connect', default=DEFAULT_ENDPOINT)
@click.argument('names', nargs=-1, metavar='name...')
def service(debug, shared_secret, connect, names):
    setup_logging(debug=debug)

    shared_secret = check_shared_secret(shared_secret)

    loop = set_event_loop()
    context = Context(loop=loop)
    socket = context.socket(azmq.DEALER)
    socket.connect(connect)

    client = Client(
        socket=socket,
        loop=loop,
    )

    registered_services = []

    for name in names:
        try:
            service_class = import_service(name)
            service = service_class(
                client=client,
                shared_secret=shared_secret,
                loop=loop,
            )

        except Exception as ex:
            click.echo(
                click.style(
                    "Unable to load service %s. Error was: %s." % (name, ex),
                    fg="yellow",
                ),
                err=True,
            )

            if debug:
                traceback.print_exc()

        else:
            registered_services.append(name)


    if not registered_services:
        click.echo(
            click.style(
                "No services were registered. Giving up.",
                fg="red",
            ),
            err=True,
        )
        client.close()
    else:
        click.echo("Service(s) %s started and connected to %s." % (
            ', '.join(registered_services),
            connect
        ))

    with allow_interruption(
        (loop, client.close),
    ):
        try:
            loop.run_until_complete(client.wait_closed())
        except Exception as ex:
            click.echo(
                click.style(
                    "Exception while running service: %s" % ex,
                    fg='red',
                ),
                err=True,
            )

    context.close()
    loop.run_until_complete(context.wait_closed())

    if registered_services:
        click.echo("Service stopped.")
    else:
        raise SystemExit(1)


@click.command()
@click.option(
    '-d',
    '--debug',
    is_flag=True,
    default=None,
    help="Enabled debug output.",
)
@click.option(
    '-s',
    '--shared-secret',
    default=None,
    help="A shared secret in base64 format that the authentication services "
    "use too.",
)
@click.option('-c', '--connect', default=[DEFAULT_ENDPOINT], multiple=True)
@click.argument('names', nargs=-1, metavar='name...')
def iservice(debug, shared_secret, connect, names):
    setup_logging(debug=debug)

    shared_secret = check_shared_secret(shared_secret)

    loop = set_event_loop()
    context = Context(loop=loop)

    clients = []
    iservices = []

    for conn in connect:
        socket = context.socket(azmq.DEALER)
        socket.connect(conn)

        client = Client(
            socket=socket,
            loop=loop,
        )
        clients.append(client)

    registered_services = []

    for name in names:
        try:
            iservice_class = import_iservice(name)
            iservice = iservice_class(
                clients=clients,
                shared_secret=shared_secret,
                loop=loop,
            )
            iservices.append(iservice)

        except Exception as ex:
            click.echo(
                click.style(
                    "Unable to load i-service %s. Error was: %s." % (name, ex),
                    fg="yellow",
                ),
                err=True,
            )

            if debug:
                traceback.print_exc()

        else:
            registered_services.append(name)


    if not registered_services:
        click.echo(
            click.style(
                "No i-services were registered. Giving up.",
                fg="red",
            ),
            err=True,
        )
        client.close()
    else:
        click.echo("I-Service(s) %s started and connected to %s." % (
            ', '.join(registered_services),
            ', '.join(connect),
        ))

    def close():
        for iservice in iservices:
            iservice.close()

        for client in clients:
            client.close()

    with allow_interruption(
        (loop, close),
    ):
        try:
            loop.run_until_complete(client.wait_closed())
        except Exception as ex:
            click.echo(
                click.style(
                    "Exception while running i-service: %s" % ex,
                    fg='red',
                ),
                err=True,
            )

    context.close()
    loop.run_until_complete(context.wait_closed())

    if registered_services:
        click.echo("I-Service stopped.")
    else:
        raise SystemExit(1)
