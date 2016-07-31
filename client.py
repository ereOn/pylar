import asyncio
import azmq

from pylar.entry_points import set_event_loop
from pylar.client import Client
from pylar.rpc_client_proxy import RPCClientProxy


async def run():
    async with azmq.Context() as context:
        async with context.socket(azmq.DEALER) as socket:
            socket.connect('tcp://127.0.0.1:3333')

            async with Client(socket=socket) as client:
                client_proxy = RPCClientProxy(
                    client=client,
                    domain=b'user/bob',
                    credentials=b'password',
                )

                arithmetic_service = await client_proxy.get_rpc_service_proxy(
                    b'service/arithmetic',
                )
                print(await arithmetic_service.sum(2, 17))


if __name__ == '__main__':
    loop = set_event_loop()
    loop.run_until_complete(run())
