"""
A client context class.
"""

from .domain import (
    from_service_domain,
    from_user_domain,
    is_service_domain,
    is_user_domain,
)


class ClientContext(object):
    """
    Represents a client context.
    """
    def __init__(self, domain, token):
        self._domain = domain
        self._token = token

    def __str__(self):
        return self.domain.decode('utf-8')

    @property
    def domain(self):
        return self._domain

    @property
    def token(self):
        return self._token

    @property
    def registered(self):
        return self._token is not None

    def is_user(self):
        return is_user_domain(self._domain)

    def is_service(self):
        return is_service_domain(self._domain)

    @property
    def username(self):
        return from_user_domain(self._domain)

    @property
    def service_name(self):
        return from_service_domain(self._domain)
