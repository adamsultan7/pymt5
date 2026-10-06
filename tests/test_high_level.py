"""Tests for the ticket-based high-level trading API.

Orchestration is tested against a real MT5WebClient with mocked
trade_request / wait_for_trade_result / symbol_info / get_positions —
no action-id bookkeeping or snapshot polling may leak to callers.
"""

import asyncio
import logging
import struct
from unittest.mock import AsyncMock, patch

import pytest

from pymt5.client import MT5WebClient
from pymt5.constants import (
    ORDER_FILLING_FOK,
    ORDER_FILLING_IOC,
    ORDER_TYPE_BUY,
    ORDER_TYPE_BUY_LIMIT,
    ORDER_TYPE_SELL,
    TRADE_ACTION_PENDING,
)
from pymt5.exceptions import MT5ConnectionError, MT5TimeoutError, SymbolNotFoundError, TradeError, ValidationError
from pymt5.transport import CommandResult
from pymt5.types import TradeResult

INFO = {"digits": 5, "volume_min": 0.01, "volume_max": 10.0, "volume_step": 0.01, "filling_mode": 1}
BUY_POS = {"position_id": 777, "trade_symbol": "EURUSD", "trade_action": 0, "trade_volume": 1000000, "digits": 5}


def _ok(deal=222, order=111) -> TradeResult:
    return TradeResult(retcode=10009, description="done", success=True, deal=deal, order=order)


def _rejected() -> TradeResult:
    return TradeResult(retcode=10013, description="Invalid request", success=False)


def _client(**stubs):
    c = MT5WebClient()
    c.symbol_info = AsyncMock(return_value=dict(INFO))
    c.get_positions = AsyncMock(return_value=[dict(BUY_POS)])
    c.trade_request = AsyncMock(return_value=_ok())
    c.wait_for_trade_result = AsyncMock(return_value={"trade_position": 555, "trade_order": 444})
    for key, value in stubs.items():
        setattr(c, key, value)
    return c


def _action_ids(c):
    """(action_id sent to trade_request, action_id waited on)."""
    sent = c.trade_request.call_args.kwargs["action_id"]
    waited = c.wait_for_trade_result.call_args.args[0]
    return sent, waited


async def test_place_market_returns_push_position_ticket():
    c = _client()
    ticket = await c.place_market("EURUSD", "buy", 0.01, sl=1.08, tp=1.09)
    assert ticket == 555
    sent, waited = _action_ids(c)
    assert sent == waited != 0
    kwargs = c.trade_request.call_args.kwargs
    assert kwargs["symbol"] == "EURUSD"
    assert kwargs["trade_type"] == ORDER_TYPE_BUY
    assert kwargs["volume"] == 1_000_000
    assert kwargs["digits"] == 5
    assert kwargs["type_filling"] == ORDER_FILLING_FOK
    assert c.trade_request.await_count == 1


async def test_place_market_sell_and_side_validation():
    c = _client()
    await c.place_market("EURUSD", "SELL", 0.01)
    assert c.trade_request.call_args.kwargs["trade_type"] == ORDER_TYPE_SELL
    with pytest.raises(ValidationError):
        await c.place_market("EURUSD", "hold", 0.01)
    assert c.trade_request.await_count == 1


async def test_place_market_unknown_symbol():
    c = _client()
    c.symbol_info = AsyncMock(return_value=None)
    with pytest.raises(SymbolNotFoundError):
        await c.place_market("NOPE", "buy", 0.01)
    c.trade_request.assert_not_awaited()


async def test_place_market_normalizes_volume():
    c = _client()
    await c.place_market("EURUSD", "buy", 0.015)  # step 0.01 -> 0.02
    assert c.trade_request.call_args.kwargs["volume"] == 2_000_000
    await c.place_market("EURUSD", "buy", 0.001)  # below min -> clamp 0.01
    assert c.trade_request.call_args.kwargs["volume"] == 1_000_000
    await c.place_market("EURUSD", "buy", 99.0)  # above max -> clamp 10.0
    assert c.trade_request.call_args.kwargs["volume"] == 1_000_000_000
    with pytest.raises(ValidationError):
        await c.place_market("EURUSD", "buy", 0.0)
    with pytest.raises(ValidationError):
        await c.place_market("EURUSD", "buy", -1.0)


async def test_place_market_filling_selection():
    c = _client()
    c.symbol_info = AsyncMock(return_value=dict(INFO, filling_mode=2))  # IOC only
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_IOC
    c.symbol_info = AsyncMock(return_value=dict(INFO, filling_mode=0))  # unknown -> FOK
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_FOK
    await c.place_market("EURUSD", "buy", 0.01, filling=ORDER_FILLING_IOC)  # explicit wins
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_IOC


async def test_place_market_rejection_raises_trade_error():
    c = _client(trade_request=AsyncMock(return_value=_rejected()))
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01)
    err = exc_info.value
    assert err.retcode == 10013
    assert err.symbol == "EURUSD"
    assert err.action == 1
    assert c.trade_request.await_count == 1


async def test_place_market_falls_back_to_cmd12_tickets():
    c = _client(wait_for_trade_result=AsyncMock(return_value=None))
    assert await c.place_market("EURUSD", "buy", 0.01) == 222  # deal preferred
    c.trade_request = AsyncMock(return_value=_ok(deal=0, order=111))
    assert await c.place_market("EURUSD", "buy", 0.01) == 111


async def test_place_market_timeout_fails_closed_without_resend():
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value=None),
    )
    with pytest.raises(MT5TimeoutError):
        await c.place_market("EURUSD", "buy", 0.01)
    assert c.trade_request.await_count == 1


async def test_place_market_connection_error_propagates():
    c = _client(trade_request=AsyncMock(side_effect=MT5ConnectionError("lost")))
    with pytest.raises(MT5ConnectionError):
        await c.place_market("EURUSD", "buy", 0.01)


async def test_close_position_by_ticket_closes_full_volume():
    c = _client()
    ticket = await c.close_position_by_ticket(777)
    assert ticket == 555
    kwargs = c.trade_request.call_args.kwargs
    assert kwargs["position_id"] == 777
    assert kwargs["symbol"] == "EURUSD"
    assert kwargs["trade_type"] == ORDER_TYPE_SELL  # opposite of BUY
    assert kwargs["volume"] == 1000000  # full protocol volume, no rescaling
    sent, waited = _action_ids(c)
    assert sent == waited != 0


async def test_close_position_by_ticket_partial_volume():
    c = _client()
    await c.close_position_by_ticket(777, volume_lots=0.02)
    assert c.trade_request.call_args.kwargs["volume"] == 2_000_000


async def test_close_position_by_ticket_unknown_ticket():
    c = _client()
    with pytest.raises(TradeError) as exc_info:
        await c.close_position_by_ticket(999)
    assert exc_info.value.retcode == 10036
    c.trade_request.assert_not_awaited()


async def test_close_position_by_ticket_rejection():
    c = _client(trade_request=AsyncMock(return_value=_rejected()))
    with pytest.raises(TradeError) as exc_info:
        await c.close_position_by_ticket(777)
    assert exc_info.value.retcode == 10013


async def test_modify_sltp_rounds_and_returns_result():
    c = _client()
    result = await c.modify_sltp(777, 1.085066, 1.090044)
    assert result.success is True
    kwargs = c.trade_request.call_args.kwargs
    assert kwargs["position_id"] == 777
    assert kwargs["price_sl"] == pytest.approx(1.08507)
    assert kwargs["price_tp"] == pytest.approx(1.09004)


async def test_modify_sltp_rejection_and_unknown_ticket():
    c = _client(trade_request=AsyncMock(return_value=_rejected()))
    with pytest.raises(TradeError) as exc_info:
        await c.modify_sltp(777, 1.08, 1.09)
    assert exc_info.value.retcode == 10013
    assert exc_info.value.action == 6
    c2 = _client()
    with pytest.raises(TradeError):
        await c2.modify_sltp(999, 1.08, 1.09)
    c2.trade_request.assert_not_awaited()


def _requote(bid=1.0850, ask=1.0852) -> TradeResult:
    return TradeResult(retcode=10004, description="Requote", success=False, bid=bid, ask=ask)


async def test_requote_default_raises_without_resend():
    c = _client(trade_request=AsyncMock(return_value=_requote()))
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01)
    assert exc_info.value.retcode == 10004
    assert c.trade_request.await_count == 1


async def test_requote_retry_resends_once_at_refreshed_price():
    c = _client(trade_request=AsyncMock(side_effect=[_requote(), _ok()]))
    ticket = await c.place_market("EURUSD", "buy", 0.01, requote_retries=1, requote_delay=0.01)
    assert ticket == 555
    assert c.trade_request.await_count == 2
    first, second = c.trade_request.call_args_list
    assert first.kwargs["price_order"] == 0.0  # market: server price
    assert second.kwargs["price_order"] == pytest.approx(1.0852)  # refreshed ask
    assert first.kwargs["action_id"] != second.kwargs["action_id"]
    waits = c.wait_for_trade_result.call_args_list
    assert [w.args[0] for w in waits] == [first.kwargs["action_id"], second.kwargs["action_id"]]


async def test_requote_exhausted_raises_last_retcode():
    c = _client(trade_request=AsyncMock(side_effect=[_requote(), _requote()]))
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01, requote_retries=1, requote_delay=0.01)
    assert exc_info.value.retcode == 10004
    assert c.trade_request.await_count == 2


async def test_requote_never_for_pending_or_close():
    c = _client(trade_request=AsyncMock(return_value=_requote()))
    with pytest.raises(TradeError):
        await c._place_with_fill(
            symbol="EURUSD",
            trade_action=TRADE_ACTION_PENDING,
            volume_proto=1000000,
            digits=5,
            filling=0,
            trade_type=ORDER_TYPE_BUY_LIMIT,
            price_order=1.08,
            requote_retries=2,
            requote_delay=0.01,
            fill_timeout=5.0,
        )
    assert c.trade_request.await_count == 1
    c2 = _client(trade_request=AsyncMock(return_value=_requote()))
    with pytest.raises(TradeError):
        await c2.close_position_by_ticket(777)
    assert c2.trade_request.await_count == 1
    import inspect

    assert "requote_retries" in inspect.signature(MT5WebClient.place_market).parameters
    assert "requote_retries" not in inspect.signature(MT5WebClient.close_position_by_ticket).parameters


async def test_requote_retry_logs_at_info(caplog):
    c = _client(trade_request=AsyncMock(side_effect=[_requote(), _ok()]))
    with caplog.at_level(logging.DEBUG, logger="pymt5.high_level"):
        await c.place_market("EURUSD", "buy", 0.01, requote_retries=1, requote_delay=0.01)
    infos = [
        r
        for r in caplog.records
        if r.name == "pymt5.high_level" and r.levelno == logging.INFO and "requote" in r.getMessage()
    ]
    assert len(infos) == 1
    assert "1.0852" in infos[0].getMessage()


async def test_requote_budget_expiry_fails_closed():
    c = _client(trade_request=AsyncMock(return_value=_requote()))
    with pytest.raises(MT5TimeoutError):
        await c.place_market("EURUSD", "buy", 0.01, requote_retries=2, requote_delay=0.3, fill_timeout=0.1)
    assert c.trade_request.await_count == 1


async def test_requote_delay_prefers_symbol_lf():
    sleeps: list[float] = []

    async def _record(delay):
        sleeps.append(delay)

    c = _client(trade_request=AsyncMock(side_effect=[_requote(), _ok()]))
    c.symbol_info = AsyncMock(return_value=dict(INFO, trade={"lf": 2.0}))
    with patch("asyncio.sleep", side_effect=_record):
        await c.place_market("EURUSD", "buy", 0.01, requote_retries=1)
    assert sleeps == [2.0]
    c2 = _client(trade_request=AsyncMock(side_effect=[_requote(), _ok()]))
    with patch("asyncio.sleep", side_effect=_record):
        await c2.place_market("EURUSD", "buy", 0.01, requote_retries=1)
    assert sleeps == [2.0, 7.0]


async def test_requote_negative_params_rejected():
    c = _client()
    with pytest.raises(ValidationError):
        await c.place_market("EURUSD", "buy", 0.01, requote_retries=-1)
    with pytest.raises(ValidationError):
        await c.place_market("EURUSD", "buy", 0.01, requote_retries=1, requote_delay=-1.0)
    c.trade_request.assert_not_awaited()


async def test_trade_request_reports_elapsed_ms():
    async def _slow_send(cmd, payload=b""):
        await asyncio.sleep(0.25)
        return CommandResult(command=cmd, code=0, body=struct.pack("<I", 10009))

    c = MT5WebClient()
    c.transport.send_command = _slow_send
    result = await c.trade_request(trade_action=1, symbol="EURUSD", volume=1000000, trade_type=0)
    assert result.success is True
    assert 200 <= result.elapsed_ms < 5000


async def test_modify_sltp_reports_elapsed_ms():
    async def _slow_send(cmd, payload=b""):
        await asyncio.sleep(0.25)
        return CommandResult(command=cmd, code=0, body=struct.pack("<I", 10009))

    c = MT5WebClient()
    c.symbol_info = AsyncMock(return_value=dict(INFO))
    c.get_positions = AsyncMock(return_value=[dict(BUY_POS)])
    c.transport.send_command = _slow_send
    result = await c.modify_sltp(777, 1.08, 1.09)
    assert 200 <= result.elapsed_ms < 5000


async def test_pending_placement_reports_elapsed_ms():
    async def _slow_send(cmd, payload=b""):
        await asyncio.sleep(0.25)
        return CommandResult(command=cmd, code=0, body=struct.pack("<I", 10009))

    c = MT5WebClient()
    c.symbol_info = AsyncMock(return_value=dict(INFO))
    c.transport.send_command = _slow_send
    result = await c.buy_limit("EURUSD", 0.01, price=1.07)
    assert 200 <= result.elapsed_ms < 5000


async def test_slow_fill_succeeds_under_default_budget():
    """A fill landing ~20s out succeeds: the 30s default covers it."""

    async def _slow_push(action_id, timeout=20.0):
        await asyncio.sleep(20.0)
        return {"trade_position": 555, "trade_order": 444}

    c = _client(wait_for_trade_result=AsyncMock(side_effect=_slow_push))
    ticket = await c.place_market("EURUSD", "buy", 0.01)
    assert ticket == 555
    budget = c.wait_for_trade_result.call_args.kwargs["timeout"]
    assert 29.0 < budget <= 30.0
    assert c.trade_request.await_count == 1


async def test_explicit_short_budget_raises_on_time_without_resend():
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value=None),
    )
    with pytest.raises(MT5TimeoutError):
        await c.place_market("EURUSD", "buy", 0.01, fill_timeout=0.05)
    budget = c.wait_for_trade_result.call_args.kwargs["timeout"]
    assert 0 < budget <= 0.05
    assert c.trade_request.await_count == 1
