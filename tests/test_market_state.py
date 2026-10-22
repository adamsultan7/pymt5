"""The market-state verdict and the stalled-stream recovery.

Two protocol-level capabilities the trading app had to re-implement:

* ``market_session_open`` - is this symbol tradable right now? The same
  verdict the official Web Terminal's ``isMarketClosed`` makes (the
  broker's trade sessions). None means "can't tell" and must never be
  read as "closed".
* ``force_resubscribe`` - a stream the server stopped pushing cannot be
  revived by subscribing again (the subscribe is a no-op for an already
  subscribed id), so the id has to be dropped first.

Plus the partial-tick marker: the server pushes only the fields that
changed, so a quote can arrive one-sided (the first ticks after a
subscribe, when there is nothing to merge with). That is not a price.
"""

import struct
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from pymt5.client import MT5WebClient
from pymt5.constants import (
    CMD_GET_FULL_SYMBOLS,
    CMD_SUBSCRIBE_TICKS,
    SYMBOL_TRADE_MODE_CLOSEONLY,
    SYMBOL_TRADE_MODE_DISABLED,
    SYMBOL_TRADE_MODE_FULL,
)
from pymt5._parsers import _parse_tick_batch
from pymt5.protocol import get_series_size
from pymt5.schemas import TICK_SCHEMA
from pymt5.transport import CommandResult


def _schedule(start_hour: int, end_hour: int, day_index: int | None = 3) -> list:
    """Seven days of trade sessions (Sunday-first), open for one window.

    The broker's shape: ``trade_sessions[day]`` is a list of
    ``(start_minute, end_minute)`` sessions. ``day_index`` 0 = Sunday
    ... 3 = Wednesday.
    """
    if day_index is None:                 # open every day
        return [[(start_hour * 60, end_hour * 60)] for _ in range(7)]
    sessions = [[(0, 0)] for _ in range(7)]
    sessions[day_index] = [(start_hour * 60, end_hour * 60)]
    return sessions


def _spec(mode=SYMBOL_TRADE_MODE_FULL, sessions=None):
    spec = {"trade": {"trade_mode": mode}}
    if sessions is not None:
        spec["schedule"] = {"trade_sessions": sessions}
    return spec


class TestMarketSessionOpen:
    @pytest.fixture
    def client(self):
        c = MT5WebClient.__new__(MT5WebClient)
        c.get_full_symbol_info = AsyncMock(return_value=None)
        c.get_account = AsyncMock(return_value={"timezone_shift": 10800})
        return c

    async def test_unknown_when_the_spec_is_unreadable(self, client):
        client.get_full_symbol_info = AsyncMock(return_value=None)
        assert await client.market_session_open("EURUSD") is None

    async def test_disabled_symbol_is_closed(self, client):
        client.get_full_symbol_info = AsyncMock(
            return_value=_spec(mode=SYMBOL_TRADE_MODE_DISABLED))
        assert await client.market_session_open("EURUSD") is False

    async def test_close_only_symbol_is_closed(self, client):
        client.get_full_symbol_info = AsyncMock(
            return_value=_spec(mode=SYMBOL_TRADE_MODE_CLOSEONLY))
        assert await client.market_session_open("EURUSD") is False

    async def test_unknown_without_sessions(self, client):
        client.get_full_symbol_info = AsyncMock(return_value=_spec())
        assert await client.market_session_open("EURUSD") is None

    async def test_open_inside_the_session(self, client):
        # Wednesday 12:00 on the broker's wall clock
        at = datetime(2026, 10, 21, 12, 0)
        client.get_full_symbol_info = AsyncMock(
            return_value=_spec(sessions=_schedule(8, 20)))
        assert await client.market_session_open("EURUSD", at) is True

    async def test_closed_outside_the_session(self, client):
        at = datetime(2026, 10, 21, 3, 0)
        client.get_full_symbol_info = AsyncMock(
            return_value=_spec(sessions=_schedule(8, 20)))
        assert await client.market_session_open("EURUSD", at) is False

    async def test_the_server_clock_is_used_by_default(self, client):
        client.get_full_symbol_info = AsyncMock(
            return_value=_spec(sessions=_schedule(0, 24, day_index=None)))
        # 10800s shift -> the server wall is 3h ahead of UTC.
        assert await client.market_session_open("EURUSD") is True


class TestForceResubscribe:
    @pytest.fixture
    def client(self):
        c = MT5WebClient.__new__(MT5WebClient)
        c._subscribed_ids = [1, 2, 3]
        sent = []

        async def _send(command, payload=None, **kwargs):
            sent.append((command, payload))
            return CommandResult(command=command, code=0, body=b"")

        c.transport = type("T", (), {"send_command": staticmethod(_send)})()
        c._sent = sent
        return c

    async def test_it_drops_and_asks_again(self, client):
        await client.force_resubscribe([2])
        commands = [cmd for cmd, _ in client._sent]
        assert commands == [CMD_SUBSCRIBE_TICKS, CMD_SUBSCRIBE_TICKS]
        # The first call drops the id, the second re-adds it.
        first = struct.unpack("<3I", client._sent[0][1][:12])
        second = struct.unpack("<4I", client._sent[1][1][:16])
        assert 2 not in first[1:]      # the id was dropped
        assert 2 in second[1:]         # and asked for again

    async def test_an_empty_list_is_a_noop(self, client):
        await client.force_resubscribe([])
        assert client._sent == []

    async def test_by_name_resolves_the_ids(self, client):
        async def _resolve(symbol):
            return {"EURUSD": 7}.get(symbol)

        client._resolve_symbol_id = _resolve
        ids = await client.force_resubscribe_symbols(["EURUSD"])
        assert ids == [7]
        assert len(client._sent) == 2


class TestPartialTick:
    def _tick_body(self, symbol_id=1, bid=1.1, ask=1.1002, last=0.0):
        from pymt5.protocol import SeriesCodec

        values = [symbol_id, 1792607859, 0, bid, ask, last, 1, 0, 0]
        return SeriesCodec.serialize(
            [(entry["propType"], value)
             for entry, value in zip(TICK_SCHEMA, values)])

    def test_a_one_sided_tick_is_marked_partial(self):
        ticks = _parse_tick_batch(self._tick_body(bid=1.1, ask=0.0), {}, {})
        assert len(ticks) == 1
        assert ticks[0].get("partial") is True

    def test_a_complete_tick_is_not_marked(self):
        ticks = _parse_tick_batch(self._tick_body(), {}, {})
        assert "partial" not in ticks[0]

    def test_the_merge_fills_the_missing_side(self):
        cache = {1: {"bid": 1.1, "ask": 1.1002, "last": 0.0}}
        ticks = _parse_tick_batch(self._tick_body(bid=1.1, ask=0.0), {}, cache)
        assert ticks[0]["ask"] == pytest.approx(1.1002)
        assert "partial" not in ticks[0]
