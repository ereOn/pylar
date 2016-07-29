"""
Domain related functions and constants.
"""


DOMAIN_SEPARATOR = b'/'
USER_DOMAIN_PREFIX = b'user'
SERVICE_DOMAIN_PREFIX = b'service'


def user_domain(username):
    """
    Fabricate a user domain.

    :param username: The username, as a string.
    :returns: The domain, as bytes.
    """
    return DOMAIN_SEPARATOR.join([
        USER_DOMAIN_PREFIX,
        username.encode('utf-8'),
    ])


def service_domain(service_name):
    """
    Fabricate a user domain.

    :param service_name: The service name, as a string.
    :returns: The domain, as bytes.
    """
    return DOMAIN_SEPARATOR.join([
        SERVICE_DOMAIN_PREFIX,
        service_name.encode('utf-8'),
    ])


def is_user_domain(domain):
    """
    Check whether a specified domain is for a user.

    :param domain: The domain to check.
    :returns: `True` if the specified domain belongs to a user.
    """
    return from_user_domain(domain) is not None


def is_service_domain(domain):
    """
    Check whether a specified domain is for a service.

    :param domain: The domain to check.
    :returns: `True` if the specified domain belongs to a user.
    """
    return from_service_domain(domain) is not None


def from_user_domain(domain):
    """
    Extract an username from a user domain.

    :param domain: The domain to extract the username from.
    :returns: The username, as a string or `None` if `domain` is not a user
        domain.
    """
    prefix = b''.join([USER_DOMAIN_PREFIX, DOMAIN_SEPARATOR])

    if domain[:len(prefix)] == prefix:
        return domain[len(prefix):]


def from_service_domain(domain):
    """
    Extract a service name from a service domain.

    :param domain: The domain to extract the service name from.
    :returns: The service name, as a string or `None` if `domain` is not a
        service domain.
    """
    prefix = b''.join([SERVICE_DOMAIN_PREFIX, DOMAIN_SEPARATOR])

    if domain[:len(prefix)] == prefix:
        return domain[len(prefix):]
