"""Type declarations for Gufi's runtime-installed native module.

Availability depends on the application's configured capabilities. This is not
a standalone Python implementation and supplies no legacy module alias.
"""

from collections.abc import Awaitable
from logging import Handler
from typing import Any, Protocol

from . import channels as channels

def invoke(operation: str, payload: Any) -> Awaitable[Any]: ...

class Counter:
    @property
    def key(self) -> str: ...
    def get(self) -> int | None: ...
    def increment(self, by: int = 1) -> int: ...
    def decrement(self, by: int = 1) -> int: ...
    def reset(self) -> None: ...

def counter(key: str) -> Counter: ...

class _Broadcast(Protocol):
    def __call__(
        self,
        channel: str | list[str],
        event: str,
        data: Any = None,
        *,
        application: str | None = None,
        socket_id: str | None = None,
    ) -> int: ...

broadcast: _Broadcast

class LogHandler(Handler):
    def __init__(self, channel: str, level: int | str = 0) -> None: ...
