"""trio/anyio no longer have queues, but sometimes a memory object stream is
unwieldy. Thus this re-implemens a simple queue using
`anyio.create_memory_object_stream`.
"""

from __future__ import annotations

import anyio
from anyio import create_memory_object_stream
from outcome import Error, Value

from typing import TYPE_CHECKING  # isort:skip

if TYPE_CHECKING:
    from collections.abc import Awaitable

import logging  # isort:skip

logger = logging.getLogger(__name__)

__all__ = [
    "Queue",
]


class Queue:
    """Queues have been replaced in trio/anyio by memory object streams, but
    those are more complicated to use.

    This Queue class simply re-implements queues on top of memory object streams.
    """

    def __init__(self, length=1):
        self._s, self._r = create_memory_object_stream(length)

    async def put(self, x):
        """Send a value, blocking"""
        await self._s.send(Value(x))

    def put_nowait(self, x):
        """Send a value, nonblocking"""
        self._s.send_nowait(Value(x))

    async def put_error(self, x):
        """Send an error value, blocking"""
        await self._s.send(Error(x))

    async def get(self):
        """Get the next value, blocking.
        May raise an exception if one was sent.
        """
        res = await self._r.receive()
        return res.unwrap()

    def get_nowait(self):
        """Get the next value, nonblocking.
        May raise an exception if one was sent.
        """
        res = self._r.receive_nowait()
        return res.unwrap()

    def qsize(self):
        """Return the number of elements in the queue"""
        return self._s.statistics().current_buffer_used

    def empty(self):
        """Check whether the queue is empty"""
        return self._s.statistics().current_buffer_used == 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        res = await self._r.__anext__()  # pylint: disable=E1101
        return res.unwrap()

    def close_sender(self):
        """No more messages will be received"""
        self._s.close()

    close_writer = close_sender

    def close_receiver(self):
        """No more messages may be sent"""
        self._r.close()

    close_reader = close_receiver
