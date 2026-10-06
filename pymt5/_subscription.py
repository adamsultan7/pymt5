"""Subscription lifecycle manager for tick and book subscriptions.

Provides :class:`SubscriptionHandle` — an async context manager that
automatically unsubscribes when exiting the ``async with`` block.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

from pymt5._logging import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger("pymt5.subscription")


class SubscriptionHandle:
    """Manages the lifecycle of a tick or book subscription.

    Usage::

        async with client.subscribe_ticks_managed([symbol_id]) as handle:
            # subscribed here
            ...
        # automatically unsubscribed here

    Or without a context manager::

        handle = await client.subscribe_ticks_managed([symbol_id])
        # ... later ...
        await handle.unsubscribe()
    """

    def __init__(
        self,
        ids: list[int],
        unsubscribe_fn: Callable[[list[int]], Coroutine[Any, Any, None]],
    ) -> None:
        self._ids = list(ids)
        self._unsubscribe_fn = unsubscribe_fn
        self._active = True

    @property
    def ids(self) -> list[int]:
        """Symbol IDs covered by this subscription."""
        return list(self._ids)

    @property
    def active(self) -> bool:
        """Whether the subscription is still active."""
        return self._active

    async def unsubscribe(self) -> None:
        """Explicitly unsubscribe. Idempotent."""
        if not self._active:
            return
        try:
            await self._unsubscribe_fn(self._ids)
        except Exception:
            logger.debug("subscription unsubscribe failed", exc_info=True)
            raise
        self._active = False

    async def __aenter__(self) -> SubscriptionHandle:
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        try:
            await self.unsubscribe()
        except Exception:
            # Never mask the body exception; log and preserve original.
            logger.debug("subscription cleanup failed on exit", exc_info=True)
            if exc_val is None:
                raise
