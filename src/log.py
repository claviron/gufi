"""`claviron.LogHandler`: the standard `logging` module on a Claviron log channel.

Configure it like any handler — Django's ``LOGGING``, ``logging.config``::

    "handlers": {
        "claviron": {"class": "claviron.LogHandler", "channel": "django"},
    }

Each record goes to the channel ``logging.yaml`` declares, through the fleet's
log ring; nothing here opens a file. The logger's name is the record's
target; ``function`` and ``line`` say where it was made; ``extra={...}`` and
an exception become fields.
"""

import logging
import sys

_claviron = sys.modules["claviron"]

# The attributes every LogRecord has. Whatever else a record carries came from
# `extra=` and becomes a field.
_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}


class LogHandler(logging.Handler):
    def __init__(self, channel, level=logging.NOTSET):
        super().__init__(level)
        self._channel = _claviron._log_channel(channel)
        if self._channel is None:
            raise ValueError(f"Claviron log channel {channel!r} is not declared in logging.yaml")

    def emit(self, record):
        try:
            fields = {
                key: value for key, value in vars(record).items() if key not in _STANDARD
            }
            fields["function"] = record.funcName
            fields["line"] = record.lineno
            if record.exc_info and record.exc_info[1] is not None:
                error = record.exc_info[1]
                fields["exception.class"] = type(error).__qualname__
                fields["exception.message"] = str(error)
                fields["exception.trace"] = self.formatException(record.exc_info)
            _claviron._log(
                self._channel,
                record.levelname,
                record.getMessage(),
                target=record.name,
                fields=fields,
            )
        except Exception:
            self.handleError(record)

    def formatException(self, exc_info):
        return logging.Formatter().formatException(exc_info)
