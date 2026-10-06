"""Tests for pure bot helpers and their mixin wrappers."""

from unittest.mock import AsyncMock

import pytest

from pymt5._parsers import normalize_price, normalize_volume, resolve_symbol
from pymt5.client import MT5WebClient
from pymt5.exceptions import SymbolNotFoundError, ValidationError
from pymt5.types import SymbolInfo


def _sym(name: str, sid: int) -> SymbolInfo:
    return SymbolInfo(name=name, symbol_id=sid, digits=5)


# ---- normalize_volume ----


def test_normalize_volume_rounds_and_clamps():
    assert normalize_volume(0.015, min_volume=0.01, max_volume=10.0, step=0.01) == pytest.approx(0.02)
    assert normalize_volume(0.001, min_volume=0.01, max_volume=10.0, step=0.01) == pytest.approx(0.01)
    assert normalize_volume(99.0, min_volume=0.01, max_volume=10.0, step=0.01) == pytest.approx(10.0)
    assert normalize_volume(1.234, step=0.0) == pytest.approx(1.234)


def test_normalize_volume_rejects():
    with pytest.raises(ValidationError):
        normalize_volume(0.0)
    with pytest.raises(ValidationError):
        normalize_volume(-1.0)
    with pytest.raises(ValidationError):
        normalize_volume("lots")


# ---- normalize_price ----


def test_normalize_price_rounds():
    assert normalize_price(1.085066, digits=5) == pytest.approx(1.08507)
    assert normalize_price(100, digits=2) == pytest.approx(100.0)
    with pytest.raises(ValidationError):
        normalize_price("high", digits=5)


# ---- resolve_symbol (pure) ----


def test_resolve_symbol_exact_then_prefix():
    cache = {"EURUSD": _sym("EURUSD", 1), "XAUUSD.sml": _sym("XAUUSD.sml", 2)}
    assert resolve_symbol(cache, "EURUSD").symbol_id == 1
    assert resolve_symbol(cache, "XAUUSD").symbol_id == 2
    assert resolve_symbol(cache, "XAUUSD.sml").symbol_id == 2


def test_resolve_symbol_unknown_ambiguous_empty():
    cache = {"EURUSD": _sym("EURUSD", 1), "EURGBP": _sym("EURGBP", 3)}
    with pytest.raises(SymbolNotFoundError):
        resolve_symbol(cache, "GBPUSD")
    with pytest.raises(SymbolNotFoundError):
        resolve_symbol(cache, "")
    with pytest.raises(SymbolNotFoundError, match="ambiguous"):
        resolve_symbol(cache, "EUR")
    assert resolve_symbol(cache, "EURUSD").symbol_id == 1  # exact wins
    with pytest.raises(SymbolNotFoundError):
        resolve_symbol(cache, None)  # type: ignore[arg-type]


def test_resolve_symbol_case_insensitive_fallback():
    cache = {"XAUUSD.SML": _sym("XAUUSD.SML", 7)}
    assert resolve_symbol(cache, "xauusd.sml").symbol_id == 7
    assert resolve_symbol(cache, "xauusd").symbol_id == 7


# ---- mixin: resolve_symbol / ensure_market_data ----


def _md_client(**stubs):
    c = MT5WebClient()
    c._symbols = {"EURUSD": _sym("EURUSD", 1), "XAUUSD.sml": _sym("XAUUSD.sml", 2)}
    c._subscribed_ids = []
    c.load_symbols = AsyncMock(return_value=dict(c._symbols))
    c.subscribe_ticks = AsyncMock(return_value=None)
    for key, value in stubs.items():
        setattr(c, key, value)
    return c


def test_mixin_resolve_symbol():
    c = _md_client()
    assert c.resolve_symbol("XAUUSD").symbol_id == 2
    with pytest.raises(SymbolNotFoundError):
        c.resolve_symbol("GBPUSD")


async def test_ensure_market_data_subscribes_missing_only():
    c = _md_client()
    c._subscribed_ids = [1]
    result = await c.ensure_market_data(["EURUSD", "XAUUSD"])
    assert result == {"EURUSD": 1, "XAUUSD": 2}
    c.subscribe_ticks.assert_awaited_once_with([2])


async def test_ensure_market_data_loads_and_rejects():
    c = _md_client()
    c._symbols = {}
    seeded = {"EURUSD": _sym("EURUSD", 1)}

    async def _load(**kwargs):
        c._symbols.update(seeded)
        return dict(c._symbols)

    c.load_symbols = AsyncMock(side_effect=_load)
    result = await c.ensure_market_data(["EURUSD"])
    assert result == {"EURUSD": 1}
    c.load_symbols.assert_awaited_once()
    with pytest.raises(SymbolNotFoundError):
        await c.ensure_market_data(["GBPUSD", "CHFJPY"])
    assert await c.ensure_market_data([]) == {}
    c.subscribe_ticks.assert_awaited_once_with([1])


# ---- mixin: get_close_reason ----

OUT_DEAL = {
    "deal": 99,
    "trade_order": 98,
    "position_id": 777,
    "trade_symbol": "EURUSD",
    "entry": 1,
    "price_close": 1.09,
    "sl": 1.08,
    "tp": 1.10,
    "comment": "",
    "trade_reason": 0,
    "time_create": 1700000000,
}


def _pos_client(**stubs):
    c = MT5WebClient()
    c.positions_get = AsyncMock(return_value=[])
    c.history_deals_get = AsyncMock(return_value=[])
    for key, value in stubs.items():
        setattr(c, key, value)
    return c


async def test_get_close_reason_open():
    c = _pos_client(positions_get=AsyncMock(return_value=[{"position_id": 777}]))
    reason, deal = await c.get_close_reason(777)
    assert (reason, deal) == ("OPEN", None)
    c.history_deals_get.assert_not_awaited()


async def test_get_close_reason_sl_tp_comment():
    c = _pos_client(history_deals_get=AsyncMock(return_value=[dict(OUT_DEAL, comment="[sl 1.08]")]))
    reason, deal = await c.get_close_reason(777)
    assert reason == "SL_HIT"
    assert deal is not None and deal["deal"] == 99
    c = _pos_client(history_deals_get=AsyncMock(return_value=[dict(OUT_DEAL, comment="tp 1.10")]))
    assert (await c.get_close_reason(777))[0] == "TP_HIT"


async def test_get_close_reason_reason_code_and_price():
    c = _pos_client(history_deals_get=AsyncMock(return_value=[dict(OUT_DEAL, trade_reason=4)]))
    assert (await c.get_close_reason(777))[0] == "SL_HIT"
    c = _pos_client(history_deals_get=AsyncMock(return_value=[dict(OUT_DEAL, trade_reason=5)]))
    assert (await c.get_close_reason(777))[0] == "TP_HIT"
    c = _pos_client(history_deals_get=AsyncMock(return_value=[dict(OUT_DEAL, price_close=1.08)]))
    assert (await c.get_close_reason(777))[0] == "SL_HIT"
    c = _pos_client(history_deals_get=AsyncMock(return_value=[dict(OUT_DEAL)]))
    assert (await c.get_close_reason(777))[0] == "CLOSED"


async def test_get_close_reason_fallback_and_unknown():
    c = _pos_client(
        history_deals_get=AsyncMock(side_effect=[[], [dict(OUT_DEAL, comment="sl")]]),
    )
    reason, deal = await c.get_close_reason(777)
    assert reason == "SL_HIT"
    assert deal is not None
    assert c.history_deals_get.await_count == 2
    second = c.history_deals_get.call_args
    assert second.args[0] > 0 and second.args[1] > second.args[0]  # explicit time range

    c = _pos_client(history_deals_get=AsyncMock(return_value=[dict(OUT_DEAL, entry=0)]))
    assert await c.get_close_reason(777) == ("UNKNOWN", None)

    c = _pos_client()
    with pytest.raises(ValidationError):
        await c.get_close_reason("abc")


async def test_get_close_reason_picks_latest_out_deal():
    older = dict(OUT_DEAL, deal=90, time_create=1699990000, comment="")
    newer = dict(OUT_DEAL, deal=99, time_create=1700000000, comment="tp")
    c = _pos_client(history_deals_get=AsyncMock(return_value=[older, newer]))
    reason, deal = await c.get_close_reason(777)
    assert reason == "TP_HIT"
    assert deal is not None and deal["deal"] == 99
