import asyncio
import azmq

from pylar.entry_points import set_event_loop
from pylar.client import Client
from pylar.rpc_client_proxy import RPCClientProxy


async def run():
    async with azmq.Context() as context:
        async with context.socket(azmq.DEALER) as socket:
            socket.connect('tcp://127.0.0.1:3333')
            client = Client(
                socket=socket,
            )
            client_proxy = RPCClientProxy(
                client=client,
                domain=b'user/bob',
                credentials=b'password',
            )
            client.register_client_proxy(client_proxy)

            try:
                await client_proxy.wait_registered()

                print(await client_proxy.describe(b'service/arithmetic'))
                r = await client_proxy.method_call(
                    target_domain=b'service/arithmetic',
                    method='sum',
                    args=[40, 2],
                )
                print(r)
            finally:
                client.close()
                await client.wait_closed()


if __name__ == '__main__':
    loop = set_event_loop()
    loop.run_until_complete(run())
