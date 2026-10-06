"""Tests for connection_stats() observability counters."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from pymt5.client import MT5WebClient
from pymt5.events import ConnectionStats


def test_stats_start_at_zero():
    stats = MT5WebClient().connection_stats()
    assert stats == ConnectionStats(
        connects=0,
        disconnects=0,
        reconnects=0,
        heartbeat_failures=0,
        last_error=None,
        last_message_age=None,
    )


async def test_stats_connect_increments():
    client = MT5WebClient()
    client.transport.connect = AsyncMock()
    await client.connect()
    await client.connect()
    stats = client.connection_stats()
    assert stats.connects == 2
    assert stats.disconnects == 0


def test_stats_disconnect_records_reason():
    client = MT5WebClient()
    client.transport.last_disconnect_reason = "boom"
    client._handle_disconnect()
    stats = client.connection_stats()
    assert stats.disconnects == 1
    assert stats.last_error == "boom"


async def test_stats_heartbeat_failures_accumulate():
    client = MT5WebClient(heartbeat_interval=0.01, heartbeat_failure_threshold=1000)
    client.ping = AsyncMock(side_effect=TimeoutError("late"))  # type: ignore[method-assign]
    client._start_heartbeat()
    await asyncio.sleep(0.05)
    client._stop_heartbeat()
    assert client.connection_stats().heartbeat_failures >= 2


async def test_stats_reconnect_counts_connect():
    from pymt5.constants import CMD_LOGIN

    client = MT5WebClient(auto_reconnect=True, max_reconnect_attempts=1, reconnect_delay=0.001)
    client._login_kwargs = {"login": 1, "password": "x"}
    client.transport.close = AsyncMock()

    new_transport = MagicMock()
    new_transport.connect = AsyncMock()
    new_transport.close = AsyncMock()
    new_transport.is_ready = True
    new_transport._on_disconnect = None
    new_transport._on_demand_recover = None
    new_transport._listeners = {}
    new_transport._callback_error_handlers = []
    new_transport.on = MagicMock()
    new_transport.last_message_age = None

    async def _fake_login(**kwargs):
        client._logged_in = True
        return ("tok", 1)

    client.login = _fake_login  # type: ignore[method-assign]
    with patch("pymt5.client.MT5WebSocketTransport", return_value=new_transport):
        await client._reconnect_loop()

    stats = client.connection_stats()
    assert stats.reconnects == 1
    assert stats.connects == 1
    assert stats.last_message_age is None
    assert CMD_LOGIN  # keep import used if refactored


def test_sync_connection_stats_and_event_registration():
    from unittest.mock import patch as _patch

    from pymt5.sync import SyncMT5Client

    with _patch("pymt5.sync.MT5WebClient") as cls:
        inst = MagicMock()
        inst.close = AsyncMock()
        inst.transport = MagicMock()
        inst.transport.is_ready = False
        cls.return_value = inst
        sync = SyncMT5Client()
        try:
            sentinel = ConnectionStats(1, 2, 3, 4, "x", None)
            inst.connection_stats = MagicMock(return_value=sentinel)
            assert sync.connection_stats() is sentinel

            handler = MagicMock()
            inst.on_trade_result_event = MagicMock(return_value=handler)
            assert sync.on_trade_result_event(lambda ev: None) is handler
            inst.on_trade_result = MagicMock(return_value=handler)
            assert sync.on_trade_result(lambda r: None) is handler
        finally:
            sync.close()


def test_connection_stats_exported():
    import pymt5

    assert "ConnectionStats" in pymt5.__all__
    assert pymt5.ConnectionStats is ConnectionStats
