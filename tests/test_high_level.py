"""Tests for the ticket-based high-level trading API.

Orchestration is tested against a real MT5WebClient with mocked
trade_request / wait_for_trade_result / symbol_info / get_positions —
no action-id bookkeeping or snapshot polling may leak to callers.
"""

from unittest.mock import AsyncMock

import pytest

from pymt5.client import MT5WebClient
from pymt5.constants import ORDER_FILLING_FOK, ORDER_FILLING_IOC, ORDER_TYPE_BUY, ORDER_TYPE_SELL
from pymt5.exceptions import MT5ConnectionError, MT5TimeoutError, SymbolNotFoundError, TradeError, ValidationError
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
