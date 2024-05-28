"""This module contains an awaitable value.
"""

from __future__ import annotations

from concurrent.futures import CancelledError

import anyio
import attr

__all__ = ["ValueEvent"]


@attr.s
class ValueEvent:
    """A waitable value useful for inter-task synchronization,
    inspired by :class:`threading.Event`.

    An event object manages an internal value, which is initially
    unset, and a task can wait for it to become True.

    Note that the value can only be set and read once.
    """

    event = attr.ib(factory=anyio.Event, init=False)
    value = attr.ib(default=None, init=False)

    def set(self, value):
        """Set the result to return this value, and wake any waiting task."""
        if self.value is not None:
            raise RuntimeError("Already set")
        self.value = value
        self.event.set()

    def set_error(self, exc):
        """Set the result to raise this exceptio, and wake any waiting task."""
        if self.value is not None:
            raise RuntimeError("Already set")
        self.value = exc
        self.event.set()

    def is_set(self):
        """Check whether the event has occurred."""
        return self.value is not None

    def cancel(self):
        """Send a cancelation to the recipient.

        Note that this is a `concurrent.futures.CancelledError`,

        not whatever the cancellation semantics of the current anyio
        backend demands.
        """
        self.set_error(CancelledError())

    async def wait(self):
        """Block until the value is set.

        If it's already set, then this method returns immediately.

        The value is not (yet) read; if it's an error, it will not be raised from here.
        """
        await self.event.wait()

    async def get(self):
        """Block until the value is set.

        If it's already set, then this method returns immediately.

        The value can only be read once.
        """
        await self.event.wait()
        if isinstance(self.value, Exception):
            raise self.value
        return self.value
