"""
Common class and utilities.
"""

import json


def serialize(value):
    """
    Serialize a value for sending over the network.

    :param value: The value to serialize.
    :returns: Bytes.
    """
    return json.dumps(value, separators=(',', ':')).encode('utf-8')


def deserialize(value):
    """
    Deserialize a value read on the network.

    :param value: The value to deserialize.
    :returns: The value.
    """
    return json.loads(value.decode('utf-8'))
