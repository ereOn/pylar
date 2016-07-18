import asyncio
import azmq

from pylar.client import Client


async def run():
    async with azmq.Context() as context:
        async with context.socket(azmq.DEALER) as socket_a:
            async with context.socket(azmq.DEALER) as socket_b:
                socket_a.connect('tcp://127.0.0.1:3333')
                socket_b.connect('tcp://127.0.0.1:3333')
                client_a = Client(socket=socket_a)
                client_b = Client(socket=socket_b)
                await client_a.register('a')
                await client_b.register('b')
                r = await client_a.call('b', 'send_message', ['hello'])
                print(r)


if __name__ == '__main__':
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
