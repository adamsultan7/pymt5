"""The cmd=4 push is a delta: a closed trade arrives with a zero volume.

The official Web Terminal removes such a trade from its book on exactly
that rule (its updatePosition: a record with no volume removes the
trade). pymt5 exposes the rule so callers do not each re-derive it - a
closed trade left in the book blocks the next entry on that pair.
"""

import struct

import pytest

from pymt5._push_handlers import _mark_closed
from pymt5.client import MT5WebClient
from pymt5.constants import CMD_GET_POSITIONS_ORDERS
from pymt5.protocol import get_series_size
from pymt5.schemas import ORDER_SCHEMA, POSITION_SCHEMA
from pymt5.transport import CommandResult


def _position_body(count: int = 1) -> bytes:
    """A counted position payload (count header + N zero-filled records).

    A zero-filled record has trade_volume == 0 - the shape the server
    sends for a CLOSED trade.
    """
    pos_size = get_series_size(POSITION_SCHEMA)
    return struct.pack("<I", count) + (b"\x00" * pos_size) * count


def _order_body(count: int = 1) -> bytes:
    order_size = get_series_size(ORDER_SCHEMA)
    return struct.pack("<I", count) + (b"\x00" * order_size) * count


class _Transport:
    def __init__(self):
        self.handlers = {}

    def on(self, command, handler):
        self.handlers[command] = handler

    async def send_command(self, command, payload=None, **kwargs):
        return CommandResult(command=command, code=0, body=b"")


class TestMarkClosed:
    def test_a_live_position_is_not_marked(self):
        records = [{"position_id": 7, "trade_volume": 1000000}]
        assert _mark_closed(records, "trade_volume") == records
        assert "closed" not in records[0]

    def test_a_closed_position_is_marked(self):
        records = [{"position_id": 7, "trade_volume": 0}]
        _mark_closed(records, "trade_volume")
        assert records[0]["closed"] is True

    def test_an_order_without_a_volume_is_marked(self):
        records = [{"trade_order": 3, "volume_current": 0}]
        _mark_closed(records, "volume_current")
        assert records[0]["closed"] is True

    def test_a_missing_volume_is_treated_as_closed(self):
        records = [{"trade_order": 3}]
        _mark_closed(records, "volume_current")
        assert records[0]["closed"] is True


class TestPushHandlers:
    def test_the_trade_update_push_marks_closed_trades(self):
        client = MT5WebClient.__new__(MT5WebClient)
        client.transport = _Transport()
        seen = {}
        client.on_trade_update(lambda data: seen.update(data))
        handler = client.transport.handlers[CMD_GET_POSITIONS_ORDERS]
        handler(CommandResult(
            command=CMD_GET_POSITIONS_ORDERS, code=0,
            body=_position_body(1) + _order_body(0)))
        assert seen["positions"][0]["closed"] is True
