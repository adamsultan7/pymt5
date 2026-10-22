"""A closed trade arrives with a zero volume - on the LIVE trade channel.

The cmd=10 trade-update push is a sequence of entries; an entry of type
2 carries a deal, the resulting position and the balance block. A closed
position arrives with ``trade_volume == 0`` (and a finished order with
``volume_current == 0``).

The official Web Terminal removes such a trade from its book on exactly
that rule (its updatePosition: a record with no volume removes the
trade). pymt5 exposes the rule so callers do not each re-derive it - a
closed trade left in the book blocks the next entry on that pair.
"""

import struct

from pymt5._push_handlers import parse_trade_update_push
from pymt5.client import MT5WebClient
from pymt5.constants import CMD_TRADE_UPDATE_PUSH
from pymt5.protocol import SeriesCodec
from pymt5.schemas import (
    DEAL_FIELD_NAMES,
    DEAL_SCHEMA,
    ORDER_FIELD_NAMES,
    ORDER_SCHEMA,
    POSITION_FIELD_NAMES,
    POSITION_SCHEMA,
    TRADE_UPDATE_BALANCE_FIELD_NAMES,
    TRADE_UPDATE_BALANCE_SCHEMA,
)
from pymt5.transport import CommandResult


def _serialize_fields(schema, names, values) -> bytes:
    """Serialize *schema* with *values* looked up by field name."""
    specs = []
    for field, name in zip(schema, names):
        spec = dict(field)
        spec["propValue"] = values.get(name, 0)
        specs.append(spec)
    return SeriesCodec.serialize(specs)


def _trade_update_body(*, position_volume: int, order_volume: int = 0) -> bytes:
    """One cmd=10 entry: a deal + the resulting position, then the balance."""
    body = struct.pack("<III", 0, 1, 0)
    body += _serialize_fields(ORDER_SCHEMA, ORDER_FIELD_NAMES,
                              {"trade_order": 11, "trade_symbol": "EURUSD",
                               "volume_current": order_volume})
    body += struct.pack("<III", 2, 2, 0)
    body += _serialize_fields(DEAL_SCHEMA, DEAL_FIELD_NAMES,
                              {"deal": 7, "trade_symbol": "EURUSD"})
    body += _serialize_fields(POSITION_SCHEMA, POSITION_FIELD_NAMES,
                              {"position_id": 42, "trade_symbol": "EURUSD",
                               "trade_volume": position_volume})
    body += _serialize_fields(TRADE_UPDATE_BALANCE_SCHEMA,
                              TRADE_UPDATE_BALANCE_FIELD_NAMES, {})
    return body


class _Transport:
    def __init__(self):
        self.handlers = {}

    def on(self, command, handler):
        self.handlers[command] = handler

    async def send_command(self, command, payload=None, **kwargs):
        return CommandResult(command=command, code=0, body=b"")


class TestParseTradeUpdatePush:
    def test_a_live_position_is_parsed_and_not_closed(self):
        entries = parse_trade_update_push(
            _trade_update_body(position_volume=1000000))
        positions = [e["position"] for e in entries if "position" in e]
        assert len(positions) == 1
        assert positions[0]["position_id"] == 42
        assert positions[0]["trade_volume"] == 1000000
        assert positions[0]["closed"] is False

    def test_a_closed_position_is_flagged(self):
        entries = parse_trade_update_push(_trade_update_body(position_volume=0))
        positions = [e["position"] for e in entries if "position" in e]
        assert positions[0]["closed"] is True

    def test_a_finished_order_is_flagged(self):
        entries = parse_trade_update_push(
            _trade_update_body(position_volume=0, order_volume=0))
        orders = [e["order"] for e in entries if "order" in e]
        assert orders[0]["trade_order"] == 11
        assert orders[0]["closed"] is True

    def test_all_three_sections_of_a_deal_entry_are_parsed(self):
        entries = parse_trade_update_push(
            _trade_update_body(position_volume=1000000))
        deal_entry = [e for e in entries if e["entry_type"] == 2][0]
        assert deal_entry["deal"]["deal"] == 7
        assert deal_entry["position"]["position_id"] == 42
        assert "balance" in deal_entry

    def test_an_unknown_entry_type_stops_parsing(self):
        body = struct.pack("<III", 99, 1, 0) + b"\x00" * 8
        assert parse_trade_update_push(body) == []

    def test_a_truncated_entry_is_not_decoded(self):
        # An entry header that promises an order record that is not there.
        body = struct.pack("<III", 0, 1, 0) + b"\x00" * 8
        assert parse_trade_update_push(body) == []

    def test_a_frame_can_carry_several_entries(self):
        # The server packs every entry of one event into a single frame
        # (e.g. "order placed" + "order filled" + "order removed").
        body = _trade_update_body(position_volume=1000000)
        body += _trade_update_body(position_volume=1000000)
        entries = parse_trade_update_push(body)
        assert len(entries) == 4
        assert [e["entry_type"] for e in entries] == [0, 2, 0, 2]


class TestPushHandlers:
    def test_the_trade_update_push_marks_closed_trades(self):
        client = MT5WebClient.__new__(MT5WebClient)
        client.transport = _Transport()
        seen = {}
        client.on_trade_update(lambda data: seen.update(data))
        handler = client.transport.handlers[CMD_TRADE_UPDATE_PUSH]
        handler(CommandResult(
            command=CMD_TRADE_UPDATE_PUSH, code=0,
            body=_trade_update_body(position_volume=0)))
        assert seen["positions"][0]["closed"] is True

    def test_the_position_update_push_reports_a_live_position(self):
        client = MT5WebClient.__new__(MT5WebClient)
        client.transport = _Transport()
        seen = []
        client.on_position_update(seen.extend)
        handler = client.transport.handlers[CMD_TRADE_UPDATE_PUSH]
        handler(CommandResult(
            command=CMD_TRADE_UPDATE_PUSH, code=0,
            body=_trade_update_body(position_volume=1000000)))
        assert len(seen) == 1
        assert seen[0]["closed"] is False
