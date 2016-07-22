"""
Authentication service.
"""

from ..errors import CallError
from ..log import logger as main_logger
from ..service import Service

logger = main_logger.getChild('authentication_service')


class AuthenticationService(Service):
    name = 'authentication'

    USER_DOMAIN_PREFIX = b'user'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._users = {}
        # TODO: Remove this.
        self.add_user('bob', 'password')

    def add_user(self, username, password):
        """
        Add or replace an user in the users database.

        :param username: The username.
        :param password: The password.
        """
        domain = (self.USER_DOMAIN_PREFIX, username.encode('utf-8'))
        self._users[domain] = password.encode('utf-8')

    def remove_user(self, username):
        """
        Remove a user from the users database.

        :param username: The username.
        """
        domain = (self.USER_DOMAIN_PREFIX, username.encode('utf-8'))
        self._users.pop(domain)

    @Service.command('authenticate')
    async def _authenticate(self, domain, token, args):
        logger.debug("Received authentication request for: %s", domain)
        password, = args
        ref_password = self._users.get(domain)

        if not ref_password:
            logger.warning(
                "Authentication failed for %s: unknown username.",
                domain,
            )
            raise CallError(
                code=401,
                message="Unknown username.",
            )

        if ref_password != password:
            logger.warning(
                "Authentication failed for %s: invalid password.",
                domain,
            )
            raise CallError(
                code=401,
                message="Invalid password.",
            )
