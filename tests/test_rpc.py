"""Local mock-server checks for the published RPC extraction."""
import asyncio
from aiohttp import web

from mint_engine.rpc import RpcPool


async def start_rpc_server(*, delay: float = 0, fail_send: bool = False,
                           missing_receipt: bool = False):
    counts = {"probes": 0, "sends": 0, "receipts": 0}

    async def handle(request):
        data = await request.json()
        method = data["method"]
        await asyncio.sleep(delay)
        if method == "eth_blockNumber":
            counts["probes"] += 1
            result = "0x1"
        elif method == "eth_getTransactionReceipt":
            counts["receipts"] += 1
            result = None if missing_receipt else {"status": "0x1"}
        elif method == "eth_sendRawTransaction":
            counts["sends"] += 1
            if fail_send:
                return web.json_response({
                    "jsonrpc": "2.0", "id": data["id"],
                    "error": {"code": -32000, "message": "simulated failure"},
                })
            result = "0x" + "ab" * 32
        else:
            result = None
        return web.json_response({"jsonrpc": "2.0", "id": data["id"], "result": result})

    app = web.Application()
    app.router.add_post("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    return runner, f"http://127.0.0.1:{port}/", counts


def test_isolated_connection_pools_and_provider_fallback():
    async def check():
        failed, failed_url, failed_count = await start_rpc_server(fail_send=True)
        working, working_url, working_count = await start_rpc_server()
        try:
            async with RpcPool([failed_url, working_url]) as rpc:
                assert rpc._send_session is not rpc._receipt_session
                assert rpc._send_session.connector is not rpc._receipt_session.connector
                tx_hash = await rpc.send_raw_transaction("0xdeadbeef")
                assert tx_hash == "0x" + "ab" * 32
                assert failed_count["sends"] >= 1
                assert working_count["sends"] >= 1
                receipt = await rpc.get_receipt(tx_hash)
                assert receipt["status"] == "0x1"
        finally:
            await failed.cleanup()
            await working.cleanup()
    asyncio.run(check())


def test_probing_and_lane_width_measurement():
    async def check():
        a, url_a, _ = await start_rpc_server(delay=0.003)
        b, url_b, _ = await start_rpc_server(delay=0.015)
        try:
            async with RpcPool([url_a, url_b]) as rpc:
                measurements = await rpc.probe_endpoints()
                assert len(measurements) == 2
                assert rpc.endpoint_order()[0] == url_a
                widths = await rpc.probe_lane_widths(rounds=3)
                assert set(widths) == {1, 2}
                assert all("p95_ms" in item and "error_rate" in item
                           for item in widths.values())
                assert all(item["samples"] > 0 for item in widths.values())
        finally:
            await a.cleanup()
            await b.cleanup()
    asyncio.run(check())


def test_null_receipt_does_not_win_race():
    async def check():
        missing, missing_url, _ = await start_rpc_server(
            delay=0, missing_receipt=True
        )
        valid, valid_url, _ = await start_rpc_server(delay=0.01)
        try:
            async with RpcPool([missing_url, valid_url]) as rpc:
                receipt = await rpc.get_receipt("0x" + "12" * 32)
                assert receipt == {"status": "0x1"}
        finally:
            await missing.cleanup()
            await valid.cleanup()
    asyncio.run(check())
