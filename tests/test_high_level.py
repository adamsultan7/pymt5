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
    # Fresh client per case: memory is empty, so the spec decides each time
    # (repeat symbols would consult memory first — see the memory tests).
    c = _client(symbol_info=AsyncMock(return_value=dict(INFO, filling_mode=2)))  # IOC only
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_IOC
    c = _client(symbol_info=AsyncMock(return_value=dict(INFO, filling_mode=0)))  # unknown -> FOK
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_FOK
    c = _client()
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


async def test_close_already_closed_raises_dedicated_subclass():
    """Item 3: POSITION_CLOSED on close is catchable as already-closed."""
    from pymt5 import PositionAlreadyClosedError

    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value=_reject_push(10036)),
    )
    with pytest.raises(PositionAlreadyClosedError) as exc_info:
        await c.close_position_by_ticket(777)
    assert exc_info.value.retcode == 10036
    assert isinstance(exc_info.value, TradeError)  # back-compat with TradeError


async def test_close_gone_before_send_is_unknown_not_closed():
    """Item 3: a book miss without explicit routing is UNKNOWN, never AlreadyClosed."""
    from pymt5 import PositionAlreadyClosedError

    c = _client(get_positions=AsyncMock(return_value=[]))
    with pytest.raises(TradeError) as exc_info:
        await c.close_position_by_ticket(999)
    assert not isinstance(exc_info.value, PositionAlreadyClosedError)
    assert exc_info.value.retcode == 10036  # retcode signal kept, type fixed
    c.trade_request.assert_not_awaited()


async def test_other_close_rejects_stay_generic_trade_error():
    """A non-10036 reject is NOT reported as already-closed."""
    from pymt5 import PositionAlreadyClosedError

    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value=_reject_push(10013)),
    )
    with pytest.raises(TradeError) as exc_info:
        await c.close_position_by_ticket(777)
    assert not isinstance(exc_info.value, PositionAlreadyClosedError)


async def test_modify_sltp_missing_position_stays_generic():
    """modify_sltp shares the lookup but not the already-closed semantics."""
    from pymt5 import PositionAlreadyClosedError

    c = _client(get_positions=AsyncMock(return_value=[]))
    with pytest.raises(TradeError) as exc_info:
        await c.modify_sltp(999, 1.0, 1.1)
    assert not isinstance(exc_info.value, PositionAlreadyClosedError)
    c.trade_request.assert_not_awaited()


def _reject_push(code=10013, order=0, position=0, **extra):
    # Canonical push record: the real verdict is ``retcode`` (from the Ep
    # response); ``action_result_code`` is a serial on build 6090+.
    push = {"trade_order": order, "trade_position": position, "retcode": code}
    push.update(extra)
    return push


async def test_zero_ticket_final_push_raises_trade_error_fast():
    import time as _time

    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value=_reject_push(10013)),
    )
    t0 = _time.monotonic()
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01, fill_timeout=60)
    assert exc_info.value.retcode == 10013
    assert _time.monotonic() - t0 < 2.0
    assert c.trade_request.await_count == 1


async def test_zero_ticket_push_surfaces_reason_details():
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(
            return_value={"trade_order": 0, "retcode": 10031, "description": "No connection"}
        ),
    )
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01)
    assert exc_info.value.retcode == 10031
    assert "No connection" in str(exc_info.value)


async def test_zero_ticket_push_without_code_is_reject_not_timeout():
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value={"trade_order": 0}),
    )
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01)
    assert exc_info.value.retcode == 0
    assert "without executing" in str(exc_info.value)


async def test_close_inherits_zero_ticket_reject():
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value=_reject_push(10013)),
    )
    with pytest.raises(TradeError) as exc_info:
        await c.close_position_by_ticket(777)
    assert exc_info.value.retcode == 10013
    assert c.trade_request.await_count == 1


async def test_pending_inherits_zero_ticket_reject():
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value=_reject_push(10013)),
    )
    with pytest.raises(TradeError) as exc_info:
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
    assert exc_info.value.retcode == 10013
    assert c.trade_request.await_count == 1


async def test_push_requote_routes_into_retry_policy():
    c = _client(
        trade_request=AsyncMock(side_effect=[_ok(deal=0, order=0), _ok()]),
        wait_for_trade_result=AsyncMock(
            side_effect=[
                {"trade_position": 0, "trade_order": 0, "retcode": 10004},
                {"trade_position": 555, "trade_order": 444},
            ]
        ),
    )
    ticket = await c.place_market("EURUSD", "buy", 0.01, requote_retries=1, requote_delay=0.01)
    assert ticket == 555
    assert c.trade_request.await_count == 2


async def test_push_requote_without_retries_is_trade_error():
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value={"trade_position": 0, "trade_order": 0, "retcode": 10004}),
    )
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01)
    assert exc_info.value.retcode == 10004
    assert c.trade_request.await_count == 1


async def test_zero_ticket_push_carries_real_retcode_not_serial():
    """Bug 2: the exception retcode is the Ep verdict, not the push serial."""
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(
            return_value={
                "trade_position": 0,
                "trade_order": 0,
                "retcode": 10036,
                "action_result_code": 12995407,  # serial from the live capture
            }
        ),
    )
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01)
    assert exc_info.value.retcode == 10036
    assert "12995407" not in str(exc_info.value)


async def test_zero_ticket_push_prefers_retcode_over_serial_key():
    """When both keys exist, ``retcode`` (Ep) wins over the serial."""
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(
            return_value={
                "trade_order": 0,
                "retcode": 10021,
                "action_result_code": 12961140,
            }
        ),
    )
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01)
    assert exc_info.value.retcode == 10021


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


# ---- Item 1: filling must be real (full-spec flags + 10030 fallback) ----


async def test_select_filling_reads_full_spec_flags():
    """FOK-only/IOC-only symbols resolve from trade_fill_flags spellings."""
    # Fresh client per case: memory is empty, so the spec decides each time.
    c = _client(symbol_info=AsyncMock(return_value=dict(INFO, filling_mode=0, trade_fill_flags=2)))
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_IOC
    # Nested under trade (full-spec shape).
    c = _client(symbol_info=AsyncMock(return_value=dict(INFO, filling_mode=0, trade={"trade_fill_flags": 2})))
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_IOC
    # FOK-only via trade_fill_flags.
    c = _client(symbol_info=AsyncMock(return_value=dict(INFO, filling_mode=0, trade_fill_flags=1)))
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_FOK
    # Both bits -> FOK preference (legacy default).
    c = _client(symbol_info=AsyncMock(return_value=dict(INFO, filling_mode=0, trade_fill_flags=3)))
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_FOK


async def test_invalid_fill_ack_fallback_retries_once_flipped():
    """10030 ack reject retries once with the alternate mode in-budget."""
    from pymt5.constants import TRADE_RETCODE_INVALID_FILL

    bad = TradeResult(retcode=TRADE_RETCODE_INVALID_FILL, description="Invalid filling", success=False)
    c = _client(trade_request=AsyncMock(side_effect=[bad, _ok()]))
    ticket = await c.place_market("EURUSD", "buy", 0.01)
    assert ticket == 555
    assert c.trade_request.await_count == 2
    first, second = c.trade_request.call_args_list
    assert first.kwargs["type_filling"] == ORDER_FILLING_FOK
    assert second.kwargs["type_filling"] == ORDER_FILLING_IOC
    assert first.kwargs["action_id"] != second.kwargs["action_id"]


async def test_invalid_fill_fails_closed_after_single_retry():
    """A second 10030 raises — no endless filling flip-flop, no resend beyond."""
    from pymt5.constants import TRADE_RETCODE_INVALID_FILL

    bad = TradeResult(retcode=TRADE_RETCODE_INVALID_FILL, description="Invalid filling", success=False)
    c = _client(trade_request=AsyncMock(side_effect=[bad, bad]))
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01)
    assert exc_info.value.retcode == TRADE_RETCODE_INVALID_FILL
    assert c.trade_request.await_count == 2


async def test_close_position_by_ticket_accepts_explicit_filling():
    """filling= passes through end to end; auto-detect otherwise."""
    c = _client()
    await c.close_position_by_ticket(777, filling=ORDER_FILLING_IOC)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_IOC
    c2 = _client()
    await c2.close_position_by_ticket(777)
    assert c2.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_FOK  # INFO mode 1


# ---- Item 2: verdict-first fill resolution (kills the echo bug) ----


async def test_invalid_fill_push_fallback_retries_once_flipped():
    """10030 push reject-echo (with the position id echoed) also flips."""
    from pymt5.constants import TRADE_RETCODE_INVALID_FILL

    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(
            side_effect=[
                {"trade_order": 0, "trade_position": 397698437, "retcode": TRADE_RETCODE_INVALID_FILL},
                {"trade_order": 0, "trade_position": 397698999, "retcode": 10009},
            ]
        ),
    )
    ticket = await c.place_market("EURUSD", "buy", 0.01)
    assert ticket == 397698999
    assert c.trade_request.await_count == 2
    first, second = c.trade_request.call_args_list
    assert first.kwargs["type_filling"] == ORDER_FILLING_FOK
    assert second.kwargs["type_filling"] == ORDER_FILLING_IOC


async def test_error_push_with_echoed_tickets_raises_not_returns():
    """Verdict first: error code wins over echo fields AND ack tickets."""
    from pymt5.constants import TRADE_RETCODE_INVALID_FILL

    c = _client()
    loop = asyncio.get_running_loop()
    waiter: asyncio.Future = loop.create_future()
    waiter.set_result({"trade_order": 0, "trade_position": 397698437, "retcode": TRADE_RETCODE_INVALID_FILL})
    with pytest.raises(TradeError) as exc_info:
        await c._resolve_fill(
            waiter=waiter,
            result=_ok(deal=999, order=888),  # ack echo must not confirm
            symbol="EURUSD",
            trade_action=1,
        )
    assert exc_info.value.retcode == TRADE_RETCODE_INVALID_FILL


async def test_error_push_echo_end_to_end_raises_trade_error():
    """Live shape: 10030 reject echoing the request position id, twice."""
    from pymt5.constants import TRADE_RETCODE_INVALID_FILL

    echo = {"trade_order": 0, "trade_position": 397698437, "retcode": TRADE_RETCODE_INVALID_FILL}
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(side_effect=[dict(echo), dict(echo)]),
    )
    with pytest.raises(TradeError) as exc_info:
        await c.place_market("EURUSD", "buy", 0.01)
    assert exc_info.value.retcode == TRADE_RETCODE_INVALID_FILL
    assert c.trade_request.await_count == 2  # one alternate-mode retry, then fail closed


async def test_success_push_resolves_via_position_ticket():
    """10009 + trade_position resolves (the live open-confirm shape)."""
    c = _client()
    loop = asyncio.get_running_loop()
    waiter: asyncio.Future = loop.create_future()
    waiter.set_result({"trade_order": 0, "trade_position": 397698437, "retcode": 10009})
    ticket = await c._resolve_fill(waiter=waiter, result=_ok(deal=0, order=0), symbol="EURUSD", trade_action=1)
    assert ticket == 397698437


async def test_success_push_without_ticket_is_timeout_not_echo():
    """Success verdict but no ticket: ack echo is not proof — reconcile."""
    c = _client()
    loop = asyncio.get_running_loop()
    waiter: asyncio.Future = loop.create_future()
    waiter.set_result({"trade_order": 0, "trade_position": 0, "retcode": 10009})
    with pytest.raises(MT5TimeoutError):
        await c._resolve_fill(waiter=waiter, result=_ok(deal=999, order=888), symbol="EURUSD", trade_action=1)


async def test_pending_success_push_resolves_order_ticket():
    """Pending placement confirms via 10008 + trade_order, not ack .order."""
    c = _client(
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value={"trade_order": 123456, "trade_position": 0, "retcode": 10008}),
    )
    ticket = await c._place_with_fill(
        symbol="EURUSD",
        trade_action=TRADE_ACTION_PENDING,
        volume_proto=1000000,
        digits=5,
        filling=0,
        trade_type=ORDER_TYPE_BUY_LIMIT,
        price_order=1.08,
        requote_retries=0,
        requote_delay=0.01,
        fill_timeout=5.0,
    )
    assert ticket == 123456


# ---- Item 3: unconditional closes, server-authoritative already-closed ----


async def test_close_cold_book_with_explicit_routing_sends():
    """Cold book + full explicit routing skips the poll and sends."""
    c = _client(get_positions=AsyncMock(return_value=[]))
    ticket = await c.close_position_by_ticket(777, symbol="EURUSD", side="buy", volume_lots=0.01)
    assert ticket == 555
    c.get_positions.assert_not_awaited()
    kwargs = c.trade_request.call_args.kwargs
    assert kwargs["symbol"] == "EURUSD"
    assert kwargs["position_id"] == 777
    assert kwargs["trade_type"] == ORDER_TYPE_SELL  # long closed with sell
    assert kwargs["volume"] == 1_000_000
    assert kwargs["type_filling"] == ORDER_FILLING_FOK  # INFO mode 1


async def test_close_cold_book_sell_side_routing():
    """Explicit side=sell closes a short with a buy."""
    c = _client(get_positions=AsyncMock(return_value=[]))
    await c.close_position_by_ticket(777, symbol="EURUSD", side="sell", volume_lots=0.01)
    assert c.trade_request.call_args.kwargs["trade_type"] == ORDER_TYPE_BUY


async def test_close_explicit_routing_validates_side():
    c = _client(get_positions=AsyncMock(return_value=[]))
    with pytest.raises(ValidationError):
        await c.close_position_by_ticket(777, symbol="EURUSD", side="hold", volume_lots=0.01)
    c.trade_request.assert_not_awaited()


async def test_close_genuinely_gone_surfaces_already_closed_from_server():
    """The server verdict — not the local lookup — declares already-closed."""
    from pymt5 import PositionAlreadyClosedError

    c = _client(
        get_positions=AsyncMock(return_value=[]),
        trade_request=AsyncMock(return_value=_ok(deal=0, order=0)),
        wait_for_trade_result=AsyncMock(return_value=_reject_push(10036)),
    )
    with pytest.raises(PositionAlreadyClosedError) as exc_info:
        await c.close_position_by_ticket(777, symbol="EURUSD", side="buy", volume_lots=0.01)
    assert exc_info.value.retcode == 10036
    assert c.trade_request.await_count == 1  # sent; the SERVER decided


async def test_close_book_miss_without_routing_is_unknown_not_closed():
    """Miss + partial routing: generic TradeError, nothing sent."""
    from pymt5 import PositionAlreadyClosedError

    c = _client(get_positions=AsyncMock(return_value=[]))
    with pytest.raises(TradeError) as exc_info:
        await c.close_position_by_ticket(999, symbol="EURUSD")  # side+volume missing
    assert not isinstance(exc_info.value, PositionAlreadyClosedError)
    c.trade_request.assert_not_awaited()


# ---- Filling memory: last working mode per symbol ----

# Spec-blind record, as live Pepperstone/ICMarkets demo return it: no
# filling/trade_mode fields, so auto-detect can only guess FOK.
BLIND_INFO = {"digits": 5, "volume_min": 0.01, "volume_max": 10.0, "volume_step": 0.01, "filling_mode": 0}


def _invalid_fill() -> TradeResult:
    return TradeResult(retcode=10030, description="Invalid filling", success=False)


def _filled_push(ticket: int = 555) -> dict:
    return {"trade_order": 0, "trade_position": ticket, "retcode": 10009}


async def test_fill_memory_records_fallback_then_hits_first_try():
    """10030 flips the memory; the next order sends it FIRST, one roundtrip."""
    c = _client(
        symbol_info=AsyncMock(return_value=dict(BLIND_INFO)),
        trade_request=AsyncMock(side_effect=[_invalid_fill(), _ok(), _ok()]),
        wait_for_trade_result=AsyncMock(return_value=_filled_push()),
    )
    assert c.fill_mode_memory == {}
    assert c.fill_mode_stats["first_try_hit_rate"] is None
    assert await c.place_market("EURUSD", "buy", 0.01) == 555
    assert c.trade_request.await_count == 2
    first, second = c.trade_request.call_args_list
    assert first.kwargs["type_filling"] == ORDER_FILLING_FOK  # blind guess
    assert second.kwargs["type_filling"] == ORDER_FILLING_IOC  # flipped
    assert c.fill_mode_memory == {"EURUSD": ORDER_FILLING_IOC}
    assert await c.place_market("EURUSD", "buy", 0.01) == 555
    assert c.trade_request.await_count == 3  # single roundtrip, no probe
    assert c.trade_request.call_args_list[-1].kwargs["type_filling"] == ORDER_FILLING_IOC
    stats = c.fill_mode_stats
    assert stats["fills"] == 2
    assert stats["first_try_fills"] == 1
    assert stats["fallback_sends"] == 1
    assert stats["first_try_hit_rate"] == pytest.approx(0.5)


async def test_fill_memory_rerecords_when_spec_changes():
    """A later 10030 on the remembered mode retries the alternate and re-records."""
    c = _client(
        symbol_info=AsyncMock(return_value=dict(BLIND_INFO)),
        trade_request=AsyncMock(side_effect=[_invalid_fill(), _ok(), _invalid_fill(), _ok()]),
        wait_for_trade_result=AsyncMock(return_value=_filled_push()),
    )
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.fill_mode_memory == {"EURUSD": ORDER_FILLING_IOC}
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.trade_request.await_count == 4
    modes = [call.kwargs["type_filling"] for call in c.trade_request.call_args_list]
    assert modes == [ORDER_FILLING_FOK, ORDER_FILLING_IOC, ORDER_FILLING_IOC, ORDER_FILLING_FOK]
    assert c.fill_mode_memory == {"EURUSD": ORDER_FILLING_FOK}


async def test_fill_memory_unknown_symbol_keeps_spec_behavior():
    """No memory -> spec lookup exactly as before, then learned."""
    c = _client(symbol_info=AsyncMock(return_value=dict(INFO, filling_mode=2)))  # IOC-only spec
    assert await c.place_market("EURUSD", "buy", 0.01) == 555
    assert c.trade_request.await_count == 1
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_IOC
    assert c.fill_mode_memory == {"EURUSD": ORDER_FILLING_IOC}


async def test_fill_memory_does_not_cross_contaminate_symbols():
    """EURUSD=IOC leaves FOK-only and blind symbols untouched."""
    c = _client(symbol_info=AsyncMock(return_value=dict(BLIND_INFO)))
    c.trade_request = AsyncMock(side_effect=[_invalid_fill(), _ok()])
    await c.place_market("EURUSD", "buy", 0.01)
    assert c.fill_mode_memory == {"EURUSD": ORDER_FILLING_IOC}
    c.symbol_info = AsyncMock(return_value=dict(INFO, filling_mode=1))  # FOK-only spec
    c.trade_request = AsyncMock(return_value=_ok())
    await c.place_market("GBPUSD", "buy", 0.01)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_FOK
    c.symbol_info = AsyncMock(return_value=dict(BLIND_INFO))  # blind too: default, not IOC
    c.trade_request = AsyncMock(return_value=_ok())
    await c.place_market("GBPUSD", "buy", 0.01)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_FOK
    assert c.fill_mode_memory == {"EURUSD": ORDER_FILLING_IOC, "GBPUSD": ORDER_FILLING_FOK}


async def test_fill_memory_amortizes_probe_over_repeat_orders():
    """Mocked 10030-on-FOK broker, 5 orders: 6 sends (2+1+1+1+1), not 10."""

    async def _broker_10030_on_fok(**kwargs):
        if kwargs.get("type_filling") == ORDER_FILLING_FOK:
            return _invalid_fill()
        return _ok()

    c = _client(
        symbol_info=AsyncMock(return_value=dict(BLIND_INFO)),
        trade_request=AsyncMock(side_effect=_broker_10030_on_fok),
        wait_for_trade_result=AsyncMock(return_value=_filled_push()),
    )
    for _ in range(5):
        assert await c.place_market("EURUSD", "buy", 0.01) == 555
    assert c.trade_request.await_count == 6
    stats = c.fill_mode_stats
    assert (stats["fills"], stats["first_try_fills"], stats["fallback_sends"]) == (5, 4, 1)
    assert stats["first_try_hit_rate"] == pytest.approx(0.8)


async def test_explicit_filling_overrides_memory():
    """Explicit filling= wins over the remembered mode on the wire."""
    c = _client(symbol_info=AsyncMock(return_value=dict(BLIND_INFO)))
    c._fill_mode_memory["EURUSD"] = ORDER_FILLING_FOK
    c.trade_request = AsyncMock(return_value=_ok())
    await c.place_market("EURUSD", "buy", 0.01, filling=ORDER_FILLING_IOC)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_IOC


async def test_close_uses_remembered_filling():
    """The close path consults memory before the spec lookup."""
    c = _client(symbol_info=AsyncMock(return_value=dict(BLIND_INFO)))
    c._fill_mode_memory["EURUSD"] = ORDER_FILLING_IOC
    await c.close_position_by_ticket(777)
    assert c.trade_request.call_args.kwargs["type_filling"] == ORDER_FILLING_IOC


async def test_fill_mode_memory_accessor_is_read_only_copy():
    c = _client()
    await c.place_market("EURUSD", "buy", 0.01)  # INFO mode 1 -> FOK, success
    mem = c.fill_mode_memory
    assert mem == {"EURUSD": ORDER_FILLING_FOK}
    mem["EURUSD"] = ORDER_FILLING_IOC
    mem["XAUUSD"] = ORDER_FILLING_IOC
    assert c.fill_mode_memory == {"EURUSD": ORDER_FILLING_FOK}
