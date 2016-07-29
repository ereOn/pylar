"""
RPC-related functions and constants.
"""

from itertools import islice
from inspect import (
    Parameter,
    Signature,
    signature,
)


PARAMETER_KINDS_STRINGS = {
    Parameter.POSITIONAL_ONLY: 'positional_only',
    Parameter.POSITIONAL_OR_KEYWORD: 'positional_or_keyword',
    Parameter.VAR_POSITIONAL: 'var_positional',
    Parameter.KEYWORD_ONLY: 'keyword_only',
    Parameter.VAR_KEYWORD: 'var_keyword',
}

PARAMETER_KINDS = dict((y, x) for (x, y) in PARAMETER_KINDS_STRINGS.items())


def serialize_parameter_kind(kind):
    return PARAMETER_KINDS_STRINGS[kind]


def deserialize_parameter_kind(kindstr):
    return PARAMETER_KINDS[kindstr]


def serialize_parameter(parameter):
    result = {
        'name': parameter.name,
        'kind': serialize_parameter_kind(parameter.kind),
    }

    if parameter.default is not Parameter.empty:
        result['default'] = parameter.default

    return result


def deserialize_parameter(parametersig):
    return Parameter(
        name=parametersig['name'],
        kind=deserialize_parameter_kind(parametersig['kind']),
        default=parametersig.get('default', Parameter.empty),
    )


def serialize_function(func, use_context):
    sig = signature(func)

    return {
        'parameters': [
            serialize_parameter(parameter)
            for parameter in islice(
                sig.parameters.values(),
                int(use_context),
                None,
            )
        ],
    }


def deserialize_function(funcsig):
    parameters = [
        deserialize_parameter(parameter)
        for parameter in funcsig['parameters']
    ]
    return Signature(parameters=parameters)
