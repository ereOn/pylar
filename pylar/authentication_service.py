"""
Authentication service.
"""

from .domain import user_domain
from .errors import CallError
from .log import logger as main_logger
from .service import Service

logger = main_logger.getChild('authentication_service')


class AuthenticationService(Service):
    name = 'authentication'

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
        domain = user_domain(username)
        self._users[domain] = password.encode('utf-8')

    def remove_user(self, username):
        """
        Remove a user from the users database.

        :param username: The username.
        """
        domain = user_domain(username)
        self._users.pop(domain)

    @Service.command(use_context=True)
    async def authenticate(self, context, password):
        """
        Authenticate the caller.

        :param context: The caller's context.
        :param password: The encoded password.
        """
        logger.debug("Received authentication request for: %s", context)
        ref_password = self._users.get(context.domain)

        if not ref_password:
            logger.warning(
                "Authentication failed for %s: unknown username.",
                context,
            )
            raise CallError(
                code=401,
                message="Unknown username.",
            )

        if ref_password != password:
            logger.warning(
                "Authentication failed for %s: invalid password.",
                context,
            )
            raise CallError(
                code=401,
                message="Invalid password.",
            )

        return [b'']
