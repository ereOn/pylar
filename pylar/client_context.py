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
        self.domain = domain
        self.token = token

    def is_user(self):
        return is_user_domain(self.domain)

    def is_service(self):
        return is_service_domain(self.domain)

    @property
    def username(self):
        return from_user_domain(self.domain)

    @property
    def service_name(self):
        return from_service_domain(self.domain)
