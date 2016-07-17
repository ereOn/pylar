"""
Errors and exceptions.
"""


class PylarError(RuntimeError):
    pass


class CallError(PylarError):
    def __init__(self, code, message):
        super().__init__(
            '%s: %s' % (code, message),
        )
        self.code = code
        self.message = message


class InvalidReplyError(CallError):
    def __init__(self):
        super().__init__(
            code=0,
            message="The received reply is invalid.",
        )


def raise_on_error(command, reply):
    try:
        code = int(reply.pop(0))
    except (IndexError, ValueError):
        raise InvalidReplyError()

    if code != 200:
        try:
            message = reply.pop(0).decode('utf-8')
        except (IndexError, UnicodeDecodeError):
            raise InvalidReplyError()
        else:
            raise CallError(
                code=code,
                message=message,
            )

    return reply


def make_error_frames(code, message):
    return [
        ('%d' % code).encode('utf-8'),
        message.encode('utf-8'),
    ]
