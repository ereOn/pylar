import asyncio
import azmq

from pylar.entry_points import set_event_loop
from pylar.client import Client
from pylar.rpc_client_proxy import RPCClientProxy


class MyClientProxy(RPCClientProxy):
    @RPCClientProxy.notification_handler(use_context=True)
    async def hello(self, context, *frames):
        print("%s says hello !" % context)


async def run(domain, endpoint):
    async with azmq.Context() as context:
        async with context.socket(azmq.DEALER) as socket:
            socket.connect(endpoint)

            async with Client(socket=socket) as client:
                client_proxy = MyClientProxy(
                    client=client,
                    domain=domain,
                    credentials=b'password',
                )
                arithmetic_service = await client_proxy.get_rpc_service_proxy(
                    b'service/arithmetic',
                )

                for x in range(60):
                    print(domain, await arithmetic_service.sum(0, x))
                    await client_proxy.notification(b'user/bob', 'hello')
                    await asyncio.sleep(1)


if __name__ == '__main__':
    loop = set_event_loop()
    loop.run_until_complete(asyncio.gather(
        run(b'user/alice', 'tcp://127.0.0.1:3333'),
        run(b'user/bob', 'tcp://127.0.0.1:3334'),
    ))
