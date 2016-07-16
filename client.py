import asyncio
import azmq


async def run():
    async with azmq.Context() as context:
        async with context.socket(azmq.DEALER) as socket:
            socket.connect('tcp://127.0.0.1:3333')
            await socket.send_multipart([b'', b'register', b'lol'])
            await asyncio.sleep(1)


if __name__ == '__main__':
    loop = asyncio.SelectorEventLoop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
