"""Django Channels' channel layer over the fleet's shared memory.

Name it in settings in place of ``channels_redis``::

    CHANNEL_LAYERS = {"default": {"BACKEND": "gufi.channels.OrbitChannelLayer"}}

Its limits are the deployment's, written in ``gufi.yaml`` under
``broadcasting.channels`` (``capacity``, ``expiry``), not in ``CONFIG``.

A consumer's channel is a box in the process that opened it, and it lives as
long as the task that opened it: the connection's. Groups reach every process
of the application.
"""

import asyncio
import sys

import msgpack
from channels.exceptions import ChannelFull
from channels.layers import BaseChannelLayer

_core = sys.modules["gufi"]._channels

# The application loop's table of waits by id, handed over by gufi's driver
# when the loop starts. A receive waits in it the way a WebSocket's does.
_waiters = None


class OrbitChannelLayer(BaseChannelLayer):
    extensions = ["groups"]

    def __init__(self, **config):
        if config:
            raise TypeError(
                "OrbitChannelLayer takes no CONFIG; write capacity and expiry "
                "in gufi.yaml under broadcasting.channels"
            )
        # The base class's own capacity and expiry are not used: the boxes
        # enforce the ones gufi.yaml declares.
        super().__init__()

    async def new_channel(self, prefix="specific"):
        name = _core.open(prefix.rstrip("."))
        task = asyncio.current_task()
        if task is not None:
            task.add_done_callback(lambda _: _core.close(name))
        return name

    async def send(self, channel, message):
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_channel_name(channel)
        if not _core.send(channel, msgpack.packb(message, use_bin_type=True)):
            raise ChannelFull(channel)

    async def receive(self, channel):
        self.require_valid_channel_name(channel)
        message = _core.receive(channel)
        if not isinstance(message, bytes):
            wait = message
            waiter = asyncio.get_running_loop().create_future()
            _waiters[wait.id] = (wait, waiter)
            try:
                message = await waiter
            finally:
                _waiters.pop(wait.id, None)
        return msgpack.unpackb(message, raw=False)

    async def group_add(self, group, channel):
        self.require_valid_group_name(group)
        self.require_valid_channel_name(channel)
        _core.group_add(group, channel)

    async def group_discard(self, group, channel):
        self.require_valid_group_name(group)
        self.require_valid_channel_name(channel)
        _core.group_discard(group, channel)

    async def group_send(self, group, message):
        assert isinstance(message, dict), "message is not a dict"
        self.require_valid_group_name(group)
        _core.group_send(group, msgpack.packb(message, use_bin_type=True))
