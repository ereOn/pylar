import asyncio
import azmq

from pylar.entry_points import set_event_loop
from pylar.client import Client
from pylar.service import Service


async def run():
    async with azmq.Context() as context:
        async with context.socket(azmq.DEALER) as socket_a:
            async with context.socket(azmq.DEALER) as socket_b:
                socket_a.connect('tcp://127.0.0.1:3333')
                socket_b.connect('tcp://127.0.0.1:3333')
                service = Service(
                    socket=socket_a,
                    shared_secret=b'mysupersecret!!!',
                    name=b'authentication',
                )
                client = Client(
                    socket=socket_b,
                    domain=(b'user', b'bob',),
                )

                try:
                    await client.register(())
                    print("client token: %r" % client.token)
                    r = await client.call(
                        domain=(b'user', b'bob'),
                        method='send_message',
                        args=['hello'],
                    )
                    print(r)
                finally:
                    service.close()
                    client.close()
                    await service.wait_closed()
                    await client.wait_closed()


if __name__ == '__main__':
    loop = set_event_loop()
    loop.run_until_complete(run())
