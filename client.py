import asyncio
import azmq

from pylar.client import Client


async def run():
    async with azmq.Context() as context:
        async with context.socket(azmq.DEALER) as socket:
            socket.connect('tcp://127.0.0.1:3333')
            client = Client(socket=socket)
            print(await client.register('my_service'))


if __name__ == '__main__':
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
