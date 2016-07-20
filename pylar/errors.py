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
