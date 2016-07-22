import asyncio
import azmq

from pylar.entry_points import set_event_loop
from pylar.client import Client


async def run():
    async with azmq.Context() as context:
        async with context.socket(azmq.DEALER) as socket:
            socket.connect('tcp://127.0.0.1:3333')
            client = Client(
                socket=socket,
                domain=(b'user', b'bob',),
                credentials=(b'password',),
            )

            try:
                await client.wait_registered()

                r = await client.method_call(
                    domain=(b'user', b'bob'),
                    method='send_message',
                    args=['hello'],
                )
                print(r)
            finally:
                client.close()
                await client.wait_closed()


if __name__ == '__main__':
    loop = set_event_loop()
    loop.run_until_complete(run())
