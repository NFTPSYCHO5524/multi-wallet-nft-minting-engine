"""Asynchronous JSON-RPC transport extracted from my multi-wallet minting system.

The public module demonstrates endpoint probing, bounded concurrent requests,
and separate HTTP connection pools for submissions and receipt polling.
"""
from __future__ import annotations

import asyncio
import itertools
import time
from typing import Any

import aiohttp

_REQUEST_IDS = itertools.count(1)


class RpcError(RuntimeError):
    def __init__(self, code: int, message: str):
        super().__init__(f"[{code}] {message}")
        self.code = code


class RpcPool:
    """Manage a small group of JSON-RPC providers with independent traffic pools."""

    def __init__(self, endpoints: list[str], *, max_inflight: int = 128,
                 timeout_seconds: float = 5.0):
        if not endpoints:
            raise ValueError("At least one RPC endpoint is required")
        self.endpoints = list(endpoints)
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._submission_slots = asyncio.Semaphore(max(1, max_inflight))
        self._receipt_slots = asyncio.Semaphore(max(1, min(64, max_inflight // 3 or 1)))
        self._send_session: aiohttp.ClientSession | None = None
        self._receipt_session: aiohttp.ClientSession | None = None
        self._probe_session: aiohttp.ClientSession | None = None
        self._latency_ms: dict[str, float] = {}
        self._failures: dict[str, int] = {url: 0 for url in endpoints}

    async def __aenter__(self):
        self._send_session = aiohttp.ClientSession(
            timeout=self._timeout,
            connector=aiohttp.TCPConnector(limit=512, limit_per_host=256),
        )
        self._receipt_session = aiohttp.ClientSession(
            timeout=self._timeout,
            connector=aiohttp.TCPConnector(limit=64, limit_per_host=32),
        )
        self._probe_session = aiohttp.ClientSession(
            timeout=self._timeout,
            connector=aiohttp.TCPConnector(limit=24, limit_per_host=4),
        )
        return self

    async def __aexit__(self, *exc):
        for session in (self._send_session, self._receipt_session, self._probe_session):
            if session is not None:
                await session.close()

    async def _call(self, endpoint: str, method: str, params: list[Any],
                    session: aiohttp.ClientSession | None) -> Any:
        if session is None:
            raise RuntimeError("Open RpcPool with 'async with' first")
        payload = {"jsonrpc": "2.0", "id": next(_REQUEST_IDS),
                   "method": method, "params": params}
        async with session.post(endpoint, json=payload) as response:
            response.raise_for_status()
            body = await response.json(content_type=None)
        if "error" in body:
            error = body["error"]
            raise RpcError(error.get("code", -1), error.get("message", "RPC error"))
        return body.get("result")

    async def probe_endpoints(self) -> list[tuple[str, float | None]]:
        """Refresh provider RTT using inexpensive eth_blockNumber reads."""
        async def one(url: str) -> tuple[str, float | None]:
            started = time.perf_counter()
            try:
                await self._call(url, "eth_blockNumber", [], self._probe_session)
            except (aiohttp.ClientError, asyncio.TimeoutError, RpcError):
                self._failures[url] += 1
                return url, None
            elapsed = (time.perf_counter() - started) * 1000
            self._latency_ms[url] = elapsed
            self._failures[url] = 0
            return url, elapsed

        return await asyncio.gather(*(one(url) for url in self.endpoints))

    def endpoint_order(self) -> list[str]:
        """Prefer responsive endpoints, keeping unmeasured ones as fallback."""
        return sorted(self.endpoints, key=lambda url: (
            self._failures[url], self._latency_ms.get(url, float("inf"))
        ))

    async def probe_lane_widths(self, rounds: int = 5) -> dict[int, dict[str, float]]:
        """Compare concurrent read profiles against the current provider set.

        This probes JSON-RPC transport, not transaction inclusion on-chain.
        """
        if rounds < 1:
            raise ValueError("rounds must be positive")
        await self.probe_endpoints()
        order = self.endpoint_order()
        scores: dict[int, dict[str, float]] = {}
        for width in range(1, min(6, len(order)) + 1):
            samples: list[float] = []
            failures = 0
            for _ in range(rounds):
                async def sample(url: str):
                    started = time.perf_counter()
                    try:
                        await self._call(url, "eth_blockNumber", [], self._probe_session)
                        return (time.perf_counter() - started) * 1000
                    except (aiohttp.ClientError, asyncio.TimeoutError, RpcError):
                        return None
                results = await asyncio.gather(*(sample(url) for url in order[:width]))
                samples.extend(r for r in results if r is not None)
                failures += sum(r is None for r in results)
            ordered = sorted(samples)
            if ordered:
                at95 = min(len(ordered) - 1, int((len(ordered) - 1) * 0.95 + 0.5))
                scores[width] = {
                    "p95_ms": ordered[at95],
                    "error_rate": failures / (failures + len(samples)),
                    "samples": float(len(samples)),
                }
            else:
                scores[width] = {"p95_ms": float("inf"), "error_rate": 1.0, "samples": 0.0}
        return scores

    async def race_read(self, method: str, params: list[Any],
                        *, require_non_null: bool = False) -> Any:
        """Take the first acceptable read result; cancel unfinished requests."""
        tasks = [asyncio.create_task(self._call(
            url, method, params, self._receipt_session
        )) for url in self.endpoint_order()[:3]]
        pending = set(tasks)
        last_error: Exception | None = None
        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    try:
                        result = task.result()
                    except (aiohttp.ClientError, asyncio.TimeoutError, RpcError) as exc:
                        last_error = exc
                        continue
                    if result is None and require_non_null:
                        continue
                    return result
            if last_error is not None:
                raise last_error
            return None
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def send_raw_transaction(self, signed_hex: str) -> str:
        """Submit a pre-signed transaction without handling private keys."""
        if not signed_hex.startswith("0x"):
            raise ValueError("A 0x-prefixed signed transaction is required")
        async with self._submission_slots:
            last_error: Exception | None = None
            for url in self.endpoint_order():
                try:
                    result = await self._call(
                        url, "eth_sendRawTransaction", [signed_hex], self._send_session
                    )
                    if not isinstance(result, str):
                        raise RpcError(-1, "Provider did not return a transaction hash")
                    return result
                except (aiohttp.ClientError, asyncio.TimeoutError, RpcError) as exc:
                    last_error = exc
            raise RpcError(-1, f"All providers failed: {last_error}")

    async def get_receipt(self, tx_hash: str) -> dict[str, Any] | None:
        async with self._receipt_slots:
            return await self.race_read(
                "eth_getTransactionReceipt", [tx_hash], require_non_null=True
            )
