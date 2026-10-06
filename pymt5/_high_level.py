"""Ticket-based trading mixin for MT5WebClient.

Small wrapper layer over :meth:`trade_request` / :meth:`wait_for_trade_result`
for bot authors who think in tickets, lots, and sides instead of protocol
integers:

- :meth:`place_market` places a market order and returns the position ticket.
- :meth:`close_position_by_ticket` closes by ticket without the caller
  resolving symbol, volume, or direction first.
- :meth:`modify_sltp` moves SL/TP by ticket.

Fill discovery (cmd-19 push correlated by action id, falling back to the
cmd-12 response tickets) stays inside these methods — callers never track
action ids or poll snapshots. Rejections raise :class:`TradeError` with
retcode/symbol/action populated; ambiguous timeouts raise
:class:`MT5TimeoutError` without resending (fail closed: reconcile with
``positions_get``).
"""

from __future__ import annotations

import asyncio
import random
from typing import TYPE_CHECKING

from pymt5._logging import get_logger
from pymt5._parsers import _validate_requested_volume
from pymt5.constants import (
    ORDER_FILLING_FOK,
    ORDER_FILLING_IOC,
    ORDER_TYPE_BUY,
    ORDER_TYPE_SELL,
    POSITION_TYPE_BUY,
    TRADE_ACTION_DEAL,
    TRADE_ACTION_SLTP,
    TRADE_RETCODE_DESCRIPTIONS,
    TRADE_RETCODE_POSITION_CLOSED,
)
from pymt5.exceptions import MT5TimeoutError, SymbolNotFoundError, TradeError, ValidationError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pymt5.types import Record, RecordList, TradeResult

logger = get_logger("pymt5.high_level")

_SIDE_TO_ORDER_TYPE = {"buy": ORDER_TYPE_BUY, "sell": ORDER_TYPE_SELL}


class _HighLevelMixin:
    """Mixin providing ticket-based trading methods for MT5WebClient."""

    if TYPE_CHECKING:
        trade_request: Callable[..., Awaitable[TradeResult]]

        async def get_positions(self) -> RecordList: ...
        async def symbol_info(self, symbol: str) -> Record | None: ...
        async def wait_for_trade_result(self, action_id: int, timeout: float = 20.0) -> Record | None: ...
        @staticmethod
        def _volume_to_lots(volume: float, precision: int = 8) -> int: ...

    def _select_filling(self, info: Record) -> int:
        """Pick type_filling from the symbol fill-flags bitmask.

        MQL5 ``SYMBOL_FILLING_FOK=1`` / ``SYMBOL_FILLING_IOC=2`` map onto
        ``ORDER_FILLING_FOK=0`` / ``ORDER_FILLING_IOC=1``. Unknown (0) keeps
        the legacy FOK default.
        """
        try:
            mode = int(info.get("filling_mode", 0) or 0)
        except (TypeError, ValueError):
            mode = 0
        if mode & 2 and not (mode & 1):
            return ORDER_FILLING_IOC
        return ORDER_FILLING_FOK

    def _normalize_lots(self, info: Record, volume_lots: float) -> float:
        """Round *volume_lots* to the symbol step and clamp to min/max."""
        try:
            volume = float(volume_lots)
        except (TypeError, ValueError):
            raise ValidationError(f"volume must be a number, got {volume_lots!r}") from None
        if not volume > 0:
            raise ValidationError(f"volume must be > 0, got {volume_lots!r}")
        try:
            step = float(info.get("volume_step", 0.0) or 0.0)
            vmin = float(info.get("volume_min", 0.0) or 0.0)
            vmax = float(info.get("volume_max", 0.0) or 0.0)
        except (TypeError, ValueError):
            step = vmin = vmax = 0.0
        if step > 0:
            volume = round(round(volume / step) * step, 8)
        if vmin > 0 and volume < vmin:
            volume = vmin
        if vmax > 0 and volume > vmax:
            volume = vmax
        error = _validate_requested_volume({"volume_min": vmin, "volume_max": vmax, "volume_step": step}, volume)
        if error is not None:
            raise ValidationError(error)
        return volume

    @staticmethod
    def _round_price(value: float | None, digits: int) -> float:
        if value is None:
            return 0.0
        return round(float(value), digits)

    @staticmethod
    def _digits_of(info: Record) -> int:
        try:
            return int(info.get("digits", 5) or 5)
        except (TypeError, ValueError):
            return 5

    async def _symbol_info_or_raise(self, symbol: str) -> Record:
        info = await self.symbol_info(symbol)
        if info is None:
            raise SymbolNotFoundError(f"unknown symbol: {symbol}")
        return info

    async def _resolve_fill(
        self,
        *,
        waiter: asyncio.Task[Record | None],
        result: TradeResult,
        symbol: str,
        trade_action: int,
    ) -> int:
        """Resolve the ticket for a successful trade request.

        Prefers the cmd-19 push (what the web UI renders: ``trade_position``,
        else ``trade_order``), falls back to the cmd-12 response tickets, and
        raises :class:`MT5TimeoutError` when neither identifies the fill.
        Never resends.
        """
        try:
            push = await waiter
        except (KeyError, ValueError, TypeError) as exc:
            logger.debug("fill wait failed: %s", exc)
            push = None
        if push:
            for key in ("trade_position", "trade_order"):
                try:
                    ticket = int(push.get(key, 0) or 0)
                except (TypeError, ValueError):
                    ticket = 0
                if ticket:
                    return ticket
        if result.deal:
            return int(result.deal)
        if result.order:
            return int(result.order)
        raise MT5TimeoutError(
            f"fill confirmation timed out for {symbol} action={trade_action}; "
            "the request may have executed — reconcile with positions_get() "
            "(no resend was attempted)"
        )

    async def _place_with_fill(
        self,
        *,
        symbol: str,
        trade_action: int,
        volume_proto: int,
        digits: int,
        filling: int,
        trade_type: int,
        price_order: float = 0.0,
        price_sl: float = 0.0,
        price_tp: float = 0.0,
        deviation: int = 0,
        comment: str = "",
        position_id: int = 0,
        fill_timeout: float = 10.0,
    ) -> int:
        action_id = random.randint(1, 2**31 - 1)
        waiter = asyncio.create_task(self.wait_for_trade_result(action_id, timeout=fill_timeout))
        try:
            result = await self.trade_request(
                action_id=action_id,
                trade_action=trade_action,
                symbol=symbol,
                volume=volume_proto,
                digits=digits,
                trade_type=trade_type,
                type_filling=filling,
                price_order=price_order,
                price_sl=price_sl,
                price_tp=price_tp,
                deviation=deviation,
                comment=comment,
                position_id=position_id,
            )
        except BaseException:
            waiter.cancel()
            raise
        if not result.success:
            waiter.cancel()
            desc = result.description or TRADE_RETCODE_DESCRIPTIONS.get(result.retcode, "")
            raise TradeError(
                f"trade rejected: {symbol} action={trade_action} retcode={result.retcode} ({desc})",
                retcode=result.retcode,
                symbol=symbol,
                action=trade_action,
            )
        return await self._resolve_fill(waiter=waiter, result=result, symbol=symbol, trade_action=trade_action)

    async def place_market(
        self,
        symbol: str,
        side: str,
        volume_lots: float,
        *,
        sl: float | None = None,
        tp: float | None = None,
        deviation: int = 20,
        comment: str = "",
        filling: int | None = None,
        fill_timeout: float = 10.0,
    ) -> int:
        """Place a market order; return the position (deal) ticket.

        Raises :class:`TradeError` on rejection and :class:`MT5TimeoutError`
        when the fill cannot be confirmed (fail closed, never resent).
        """
        try:
            order_type = _SIDE_TO_ORDER_TYPE[str(side).strip().lower()]
        except (KeyError, AttributeError):
            raise ValidationError(f"side must be 'buy' or 'sell', got {side!r}") from None
        info = await self._symbol_info_or_raise(symbol)
        digits = self._digits_of(info)
        return await self._place_with_fill(
            symbol=symbol,
            trade_action=TRADE_ACTION_DEAL,
            volume_proto=self._volume_to_lots(self._normalize_lots(info, volume_lots)),
            digits=digits,
            filling=filling if filling is not None else self._select_filling(info),
            trade_type=order_type,
            price_sl=self._round_price(sl, digits),
            price_tp=self._round_price(tp, digits),
            deviation=deviation,
            comment=comment,
            fill_timeout=fill_timeout,
        )

    async def _position_or_raise(self, ticket: int) -> Record:
        positions = await self.get_positions()
        for position in positions:
            try:
                if int(position.get("position_id", 0) or 0) == int(ticket):
                    return position
            except (TypeError, ValueError):
                continue
        raise TradeError(
            f"position {ticket} not found among open positions (already closed?)",
            retcode=TRADE_RETCODE_POSITION_CLOSED,
            symbol="",
            action=TRADE_ACTION_DEAL,
        )

    async def close_position_by_ticket(
        self,
        ticket: int,
        *,
        deviation: int = 20,
        comment: str = "",
        volume_lots: float | None = None,
        fill_timeout: float = 10.0,
    ) -> int:
        """Close an open position by ticket; return the closing deal ticket."""
        position = await self._position_or_raise(ticket)
        symbol = str(position.get("trade_symbol", "") or "")
        if not symbol:
            raise TradeError(
                f"position {ticket} has no symbol",
                retcode=TRADE_RETCODE_POSITION_CLOSED,
                symbol="",
                action=TRADE_ACTION_DEAL,
            )
        info = await self._symbol_info_or_raise(symbol)
        is_buy = int(position.get("trade_action", 0) or 0) == POSITION_TYPE_BUY
        if volume_lots is None:
            try:
                volume_proto = int(position.get("trade_volume", 0) or 0)
            except (TypeError, ValueError):
                volume_proto = 0
            if volume_proto <= 0:
                raise TradeError(
                    f"position {ticket} has no volume",
                    retcode=TRADE_RETCODE_POSITION_CLOSED,
                    symbol=symbol,
                    action=TRADE_ACTION_DEAL,
                )
        else:
            volume_proto = self._volume_to_lots(self._normalize_lots(info, volume_lots))
        return await self._place_with_fill(
            symbol=symbol,
            trade_action=TRADE_ACTION_DEAL,
            volume_proto=volume_proto,
            digits=self._digits_of(info),
            filling=self._select_filling(info),
            trade_type=ORDER_TYPE_SELL if is_buy else ORDER_TYPE_BUY,
            deviation=deviation,
            comment=comment,
            position_id=int(ticket),
            fill_timeout=fill_timeout,
        )

    async def modify_sltp(self, ticket: int, sl: float, tp: float) -> TradeResult:
        """Move SL/TP of an open position by ticket; return the raw result.

        Raises :class:`TradeError` when the position is unknown or the server
        rejects the modification.
        """
        position = await self._position_or_raise(ticket)
        symbol = str(position.get("trade_symbol", "") or "")
        info = await self._symbol_info_or_raise(symbol)
        digits = self._digits_of(info)
        result = await self.trade_request(
            trade_action=TRADE_ACTION_SLTP,
            symbol=symbol,
            position_id=int(ticket),
            price_sl=self._round_price(sl, digits),
            price_tp=self._round_price(tp, digits),
        )
        if not result.success:
            desc = result.description or TRADE_RETCODE_DESCRIPTIONS.get(result.retcode, "")
            raise TradeError(
                f"modify rejected: {symbol} position={ticket} retcode={result.retcode} ({desc})",
                retcode=result.retcode,
                symbol=symbol,
                action=TRADE_ACTION_SLTP,
            )
        return result
