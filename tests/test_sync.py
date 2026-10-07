"""Tests for SyncMT5Client: blocking facade over a mocked async client.

Zero asyncio in the test bodies themselves — the background loop thread is
real, the wrapped MT5WebClient is mocked.
"""

import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pymt5.exceptions import MT5ConnectionError, MT5TimeoutError, TradeError
from pymt5.sync import SyncMT5Client
from pymt5.types import SymbolInfo, TradeResult


def _make_sync(**kwargs):
    """Build a SyncMT5Client around a mocked async client. Caller must close."""
    inst = MagicMock()
    inst.connect = AsyncMock(return_value=inst)
    inst.close = AsyncMock(return_value=None)
    inst.transport = MagicMock()
    inst.transport.is_ready = False
    with patch("pymt5.sync.MT5WebClient", return_value=inst):
        sync = SyncMT5Client(**kwargs)
    return sync, inst


def _ok_trade() -> TradeResult:
    return TradeResult(retcode=10009, description="done", success=True, deal=777, order=888)


def test_sync_end_to_end_connect_subscribe_place_close():
    sync, inst = _make_sync()
    try:
        inst.login = AsyncMock(return_value=("tok", 42))
        inst.load_symbols = AsyncMock(return_value={"EURUSD": SymbolInfo("EURUSD", 1, 5)})
        inst.subscribe_symbols = AsyncMock(return_value=[1])
        inst.subscribe_ticks = AsyncMock(return_value=None)
        inst.buy_market = AsyncMock(return_value=_ok_trade())
        inst.positions_get = AsyncMock(return_value=[{"position_id": 777}])

        sync.connect()
        assert sync.is_connected is inst.is_connected
        token, session = sync.login(1, "pw")
        assert (token, session) == ("tok", 42)
        assert sync.load_symbols() == {"EURUSD": SymbolInfo("EURUSD", 1, 5)}
        assert sync.subscribe_symbols(["EURUSD"]) == [1]
        result = sync.buy_market("EURUSD", 0.01)
        assert result.deal == 777
        assert sync.positions_get() == [{"position_id": 777}]
    finally:
        sync.close()
    assert sync._thread is None
    inst.close.assert_awaited_once()


def test_sync_context_manager_closes():
    with patch("pymt5.sync.MT5WebClient") as cls:
        inst = MagicMock()
        inst.connect = AsyncMock(return_value=inst)
        inst.close = AsyncMock(return_value=None)
        inst.transport = MagicMock()
        inst.transport.is_ready = False
        cls.return_value = inst
        with SyncMT5Client() as sync:
            assert sync.async_client is inst
        inst.close.assert_awaited_once()


def test_sync_per_call_timeout_raises_mt5timeout():
    import asyncio as _aio

    sync, inst = _make_sync(timeout=30.0)
    try:

        async def _slow():
            await _aio.sleep(5)

        inst.ping = AsyncMock(side_effect=_slow)
        with pytest.raises(MT5TimeoutError):
            sync.ping(timeout=0.05)
    finally:
        sync.close()


def test_sync_errors_are_loud_not_swallowed():
    sync, inst = _make_sync()
    try:
        inst.buy_market = AsyncMock(side_effect=TradeError("rejected", retcode=10013, symbol="EURUSD", action=1))
        with pytest.raises(TradeError) as exc_info:
            sync.buy_market("EURUSD", 0.01)
        assert exc_info.value.retcode == 10013

        inst.positions_get = AsyncMock(side_effect=MT5ConnectionError("lost"))
        with pytest.raises(MT5ConnectionError):
            sync.positions_get()
    finally:
        sync.close()


def test_sync_close_idempotent_and_closed_calls_raise():
    sync, inst = _make_sync()
    sync.close()
    sync.close()
    inst.close.assert_awaited_once()
    with pytest.raises(MT5ConnectionError, match="closed"):
        sync.ping(timeout=1.0)
    with pytest.raises(MT5ConnectionError, match="closed"):
        sync.positions_get(timeout=1.0)


def test_sync_connect_single_flight_across_threads():
    import asyncio as _aio

    sync, inst = _make_sync()
    try:
        calls = 0

        async def _connect_once():
            nonlocal calls
            calls += 1
            await _aio.sleep(0.05)
            inst.transport.is_ready = True
            return inst

        inst.connect = _connect_once
        errors: list = []

        def _work():
            try:
                sync.connect()
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=_work) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
        assert not errors
        assert calls == 1
    finally:
        sync.close()


def test_sync_wait_for_trade_result_passthrough():
    sync, inst = _make_sync()
    try:
        inst.wait_for_trade_result = AsyncMock(return_value={"action_id": 7, "trade_order": 888})
        assert sync.wait_for_trade_result(7) == {"action_id": 7, "trade_order": 888}
        inst.wait_for_trade_result = AsyncMock(return_value=None)
        assert sync.wait_for_trade_result(7, timeout=0.1) is None
    finally:
        sync.close()


def test_sync_local_reads_and_handler_registration():
    sync, inst = _make_sync()
    try:
        inst.get_symbol_info = MagicMock(return_value=SymbolInfo("EURUSD", 1, 5))
        assert sync.get_symbol_info("EURUSD") == SymbolInfo("EURUSD", 1, 5)
        inst.last_error = MagicMock(return_value=(0, ""))
        assert sync.last_error() == (0, "")

        handler = MagicMock()
        inst.on_tick = MagicMock(return_value=handler)
        assert sync.on_tick(lambda ticks: None) is handler
    finally:
        sync.close()


def test_sync_ticket_api_mirrors():
    from pymt5.types import TradeResult as _TradeResult

    sync, inst = _make_sync()
    try:
        inst.place_market = AsyncMock(return_value=555)
        assert sync.place_market("EURUSD", "buy", 0.01) == 555
        inst.place_market.assert_awaited_once()

        inst.close_position_by_ticket = AsyncMock(return_value=666)
        assert sync.close_position_by_ticket(555) == 666

        ok = _TradeResult(retcode=0, description="OK", success=True)
        inst.modify_sltp = AsyncMock(return_value=ok)
        assert sync.modify_sltp(555, 1.08, 1.09) is ok
    finally:
        sync.close()


def test_sync_helper_mirrors():
    from pymt5.types import SymbolInfo as _SymbolInfo

    sync, inst = _make_sync()
    try:
        inst.resolve_symbol = MagicMock(return_value=_SymbolInfo("XAUUSD.sml", 2, 3))
        assert sync.resolve_symbol("XAUUSD") == _SymbolInfo("XAUUSD.sml", 2, 3)

        inst.ensure_market_data = AsyncMock(return_value={"EURUSD": 1})
        assert sync.ensure_market_data(["EURUSD"]) == {"EURUSD": 1}

        inst.get_close_reason = AsyncMock(return_value=("SL_HIT", {"deal": 99}))
        assert sync.get_close_reason(777) == ("SL_HIT", {"deal": 99})
    finally:
        sync.close()


def test_sync_reports_elapsed_ms():
    from pymt5.types import TradeResult as _TradeResult

    sync, inst = _make_sync()
    try:
        stamped = _TradeResult(retcode=10009, description="done", success=True, elapsed_ms=251.0)
        inst.modify_sltp = AsyncMock(return_value=stamped)
        assert sync.modify_sltp(777, 1.08, 1.09).elapsed_ms == 251.0
    finally:
        sync.close()


def test_sync_zero_ticket_push_raises_trade_error():
    from pymt5.client import MT5WebClient as _AsyncClient
    from pymt5.exceptions import TradeError as _TradeError
    from pymt5.types import TradeResult as _TradeResult

    real = _AsyncClient()
    real.symbol_info = AsyncMock(
        return_value={"digits": 5, "volume_min": 0.01, "volume_max": 10.0, "volume_step": 0.01}
    )
    real.trade_request = AsyncMock(
        return_value=_TradeResult(retcode=10009, description="done", success=True, deal=0, order=0)
    )
    real.wait_for_trade_result = AsyncMock(
        return_value={"trade_order": 0, "action_result_code": 10031, "description": "No connection"}
    )
    with patch("pymt5.sync.MT5WebClient", return_value=real):
        sync = SyncMT5Client()
    try:
        with pytest.raises(_TradeError) as exc_info:
            sync.place_market("EURUSD", "buy", 0.01, timeout=5)
        assert exc_info.value.retcode == 10031
    finally:
        sync.close()


def test_sync_close_forwards_filling():
    """Item 1: close_position_by_ticket accepts filling= end to end."""
    sync, inst = _make_sync()
    try:
        inst.close_position_by_ticket = AsyncMock(return_value=666)
        assert sync.close_position_by_ticket(555, filling=1) == 666
        inst.close_position_by_ticket.assert_awaited_once_with(
            555, deviation=20, comment="", volume_lots=None, fill_timeout=30.0, filling=1, symbol=None, side=None
        )
    finally:
        sync.close()


def test_sync_close_forwards_explicit_routing():
    """Item 3: symbol/side reach the async close for stale-book sends."""
    sync, inst = _make_sync()
    try:
        inst.close_position_by_ticket = AsyncMock(return_value=666)
        assert sync.close_position_by_ticket(555, symbol="EURUSD", side="buy", volume_lots=0.01) == 666
        inst.close_position_by_ticket.assert_awaited_once_with(
            555,
            deviation=20,
            comment="",
            volume_lots=0.01,
            fill_timeout=30.0,
            filling=None,
            symbol="EURUSD",
            side="buy",
        )
    finally:
        sync.close()


def test_sync_fill_mode_accessors():
    """Filling memory + stats are local reads on the sync mirror."""
    sync, inst = _make_sync()
    try:
        inst.fill_mode_memory = {"EURUSD": 1}
        assert sync.fill_mode_memory == {"EURUSD": 1}
        inst.fill_mode_stats = {"fills": 2, "first_try_hit_rate": 0.5}
        assert sync.fill_mode_stats["first_try_hit_rate"] == 0.5
    finally:
        sync.close()
