"""
A client class.
"""

import asyncio

from math import ceil

from .client_proxy import ClientProxy
from .errors import CallError
from .generic_client import GenericClient
from .log import logger as main_logger

logger = main_logger.getChild('client')


class Client(GenericClient):
    def __init__(self, *, socket, **kwargs):
        super().__init__(**kwargs)
        self.socket = socket
        self._token = None
        self._registered = asyncio.Event(loop=self.loop)
        self._unregistered = asyncio.Event(loop=self.loop)
        self._unregistered.set()
        self.__ping_timeout = 5.0
        self.__ping_interval = 5.0
        self.__has_connection = asyncio.Event(loop=self.loop)
        self.__has_client_proxies = asyncio.Event(loop=self.loop)
        self.__client_proxies = set()
        self.__client_proxies_by_domain = {}
        self.__remote_uid = None
        self.add_task(self.__ping_loop())

    @property
    def has_connection(self):
        return self.__has_connection.is_set()

    async def wait_connection(self):
        await self.__has_connection.wait()

    def register_client_proxy(self, client_proxy):
        assert client_proxy not in self.__client_proxies, (
            "This client proxy was already registered."
        )
        assert client_proxy.domain not in self.__client_proxies_by_domain, (
            "A client proxy with the same domain was already registered."
        )

        self.__client_proxies.add(client_proxy)
        self.__client_proxies_by_domain[client_proxy.domain] = client_proxy
        self.__has_client_proxies.set()
        self.add_cleanup(client_proxy.close)
        self.add_cleanup(client_proxy.wait_closed)

    def unregister_client_proxy(self, client_proxy):
        assert client_proxy in self.__client_proxies

        self.__client_proxies.remove(client_proxy)
        del self.__client_proxies_by_domain[client_proxy.domain]

        if not self.__client_proxies:
            self.__has_client_proxies.clear()

        client_proxy.close()

    @property
    def client_proxies(self):
        return list(self.__client_proxies)

    @property
    def active_client_proxies(self):
        return [
            client_proxy for client_proxy in self.client_proxies
            if client_proxy.registered
        ]

    def get_client_proxy(self, domain):
        """
        Get an active client proxy with the specified domain.

        :param domain: The domain.
        :returns: The client proxy, or `None` if no such client proxy is found.
        """
        return self.__client_proxies_by_domain.get(domain)

    async def request(self, source_domain, target_domain, command, args=()):
        """
        Send a generic request to a specified domain.

        :param source domain: The source domain.
        :param target_domain: The target domain.
        :param command: The command.
        :param args: A list of frames to pass.
        :returns: The request result.
        """
        frames = [
            b'request',
            source_domain,
            target_domain,
            command.encode('utf-8'),
        ]
        frames.extend(args)

        return await self._request(frames)

    async def notification(self, source_domain, target_domain, type_, args=()):
        """
        Send a generic notification to a specified domain.

        :param source domain: The source domain.
        :param target_domain: The target domain.
        :param type_: The type.
        :param args: A list of frames to pass.
        """
        frames = [
            source_domain,
            target_domain,
            type_.encode('utf-8'),
        ]
        frames.extend(args)

        return await self._notification(frames)

    async def query(self, source_domain, target_domain):
        """
        Query the broker for a given domain.

        :param domain: The domain that queries.
        :param target_domain: The target domain to query.
        """
        frames = [b'query', source_domain, target_domain]

        return await self._request(frames)

    async def transmit(
        self,
        source_domain,
        target_domain,
        x_domain,
        x_token,
        frames,
    ):
        """
        Query the broker for a given domain.

        :param domain: The domain that queries.
        :param target_domain: The target domain to query.
        """
        frames = [
            b'transmit',
            source_domain,
            target_domain,
            x_domain,
            x_token,
        ] + list(frames)

        return await self._request(frames)

    # Protected methods.

    async def _read(self):
        """
        Read frames.

        :returns: The read frames.
        """
        frames = await self.socket.recv_multipart()
        frames.pop(0)  # Empty frame.

        return frames

    async def _write(self, frames):
        """
        Write frames.

        :param frames: The frames to write.
        """
        frames.insert(0, b'')
        await self.socket.send_multipart(frames)

    async def _register(self, domain, credentials):
        """
        Register on the broker.

        :param domain: The domain to register for.
        :param credentials: The credentials for the domain.
        :returns: The authentication token.
        """
        frames = [b'register', domain, credentials]
        token, = await self._request(frames)

        return token

    async def _unregister(self, domain):
        """
        Unregister from the broker.
        """
        frames = [b'unregister', domain]

        await self._request(frames)

    async def _ping(self):
        """
        Ping the broker.
        """
        remote_uid, = await self._request([b'ping'])

        return remote_uid

    async def _on_request(self, frames):
        """
        Called whenever a request is received.

        :param frames: The request frames.
        :returns: A list of frames that constitute the reply.
        """
        domain = frames.pop(0)
        client_proxy = self.__client_proxies_by_domain.get(domain)

        if not client_proxy:
            raise CallError(
                code=404,
                message="Client not found.",
            )

        source_domain = frames.pop(0)
        source_token = frames.pop(0)
        command = frames.pop(0)

        return await client_proxy.on_request(
            source_domain,
            source_token,
            command.decode('utf-8'),
            frames,
        )

    async def _on_notification(self, frames):
        """
        Called whenever a notification is received.

        :param frames: The request frames.
        """
        try:
            domain = frames.pop(0)
            client_proxy = self.__client_proxies_by_domain.get(domain)

            if not client_proxy:
                raise CallError(
                    code=404,
                    message="Client not found.",
                )

            source_domain = frames.pop(0)
            source_token = frames.pop(0)
            type_ = frames.pop(0)

            await client_proxy.on_notification(
                source_domain,
                source_token,
                type_.decode('utf-8'),
                frames,
            )
        except Exception as ex:
            logger.exception(
                "Unexpected error while handling an incoming notification.",
            )

    async def __reset(self):
        # Flush the outgoing queues.
        self.__has_connection.clear()
        self.__remote_uid = None

        for client_proxy in self.client_proxies:
            client_proxy.token = None

        await self.socket.reset_all()

    async def __ping_loop(self):
        while not self.closing:
            await self.__has_client_proxies.wait()

            try:
                remote_uid = await asyncio.wait_for(
                    self._ping(),
                    self.__ping_timeout,
                )
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                if self.active_client_proxies:
                    logger.warning(
                        "Broker did not reply in %s second(s). Performing "
                        "implicit unregistration.",
                        self.__ping_timeout,
                    )

                await self.__reset()

            except Exception as ex:
                if self.active_client_proxies:
                    logger.error(
                        "Ping request failed (%s). Performing implicit "
                        "unregistration.",
                        ex,
                    )

                await self.__reset()
            else:
                if self.__remote_uid is None:
                    self.__remote_uid = remote_uid
                elif self.__remote_uid != remote_uid:
                    logger.warning(
                        "Broker unique identifier changed ! Performing "
                        "implicit unregistration.",
                    )
                    await self.__reset()

                    # Let's not sleep when we know the connection is alive.
                    continue

                self.__has_connection.set()

            await asyncio.sleep(self.__ping_interval, loop=self.loop)
