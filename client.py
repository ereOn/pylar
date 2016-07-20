import asyncio
import azmq

from pylar.entry_points import set_event_loop
from pylar.client import Client


async def run():
    async with azmq.Context() as context:
        async with context.socket(azmq.DEALER) as socket_a:
            async with context.socket(azmq.DEALER) as socket_b:
                socket_a.connect('tcp://127.0.0.1:3333')
                socket_b.connect('tcp://127.0.0.1:3333')
                client_a = Client(
                    socket=socket_a,
                    domain=(b'user', b'alice',),
                    credentials=(),
                )
                client_b = Client(
                    socket=socket_b,
                    domain=(b'user', b'bob',),
                    credentials=(),
                )
                await asyncio.wait_for(client_a.register(), 1)
                await client_b.register()
                r = await client_a.call(
                    domain=(b'user', b'bob'),
                    method='send_message',
                    args=['hello'],
                )
                print(r)


if __name__ == '__main__':
    loop = set_event_loop()
    loop.run_until_complete(run())
