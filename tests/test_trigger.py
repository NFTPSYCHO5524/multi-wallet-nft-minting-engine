"""Tests for the dependency-free trigger extraction."""
import asyncio
from datetime import datetime, timedelta, timezone

from mint_engine.trigger import wait_for_condition, wait_until


def test_time_trigger():
    async def check():
        await asyncio.wait_for(wait_until(
            datetime.now(timezone.utc) + timedelta(milliseconds=15)
        ), timeout=1)
    asyncio.run(check())


def test_condition_retries_after_temporary_failure():
    async def check():
        calls = 0

        async def probe():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary network failure")
            return calls >= 3

        await asyncio.wait_for(wait_for_condition(probe, 0.001), timeout=1)
        assert calls == 3
    asyncio.run(check())
