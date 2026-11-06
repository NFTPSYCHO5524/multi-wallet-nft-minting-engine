"""Reusable time- and state-based asynchronous triggers.

These functions are extracted from the scheduler pattern used in my mintbot.
They do not load wallet keys or execute transactions.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone


async def wait_until(target: datetime, clock_offset_ms: int = 0) -> None:
    """Sleep until an absolute UTC time, checking more frequently near the end."""
    if target.tzinfo is None:
        target = target.replace(tzinfo=timezone.utc)
    deadline = target.timestamp() + clock_offset_ms / 1000
    coarse_delay = deadline - time.time() - 1.0
    if coarse_delay > 0:
        await asyncio.sleep(coarse_delay)
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        await asyncio.sleep(min(0.002, remaining))


async def wait_for_condition(check: Callable[[], Awaitable[bool]],
                             interval_seconds: float = 0.1) -> None:
    """Poll an asynchronous condition without stopping on temporary exceptions."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    while True:
        try:
            if await check():
                return
        except Exception:
            pass
        await asyncio.sleep(interval_seconds)
