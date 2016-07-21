"""
Security utils.
"""

from csodium import (
    crypto_generichash_blake2b_salt_personal,
    randombytes,
)


def generate_salt():
    """
    Generates a random salt.

    :returns: A random salt.
    """
    return randombytes(16)


def generate_hash(shared_secret, salt, identifier):
    """
    Generate a secure hash for shared-secret checking purposes.
    """
    personal = identifier + b'-' * (16 - len(identifier))

    return crypto_generichash_blake2b_salt_personal(
        in_=None,
        key=shared_secret,
        salt=salt,
        personal=personal,
    )


def verify_hash(shared_secret, salt, identifier, hash):
    """
    Verify a secure hash.
    """
    reference = generate_hash(shared_secret, salt, identifier)

    return hash == reference
