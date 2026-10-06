"""Ticket-based trading mixin for MT5WebClient.

Small wrapper layer over :meth:`trade_request` / :meth:`wait_for_trade_result`
for bot authors who think in tickets, lots, and sides instead of protocol
integers:

- :meth:`place_market` places a market order and returns the position ticket.
- :meth:`close_position_by_ticket` closes by ticket without the caller
  resolving symbol, volume, or direction first.
- :meth:`modify_sltp` moves SL/TP by ticket.

Bot helpers on the same mixin: :meth:`resolve_symbol` (broker-suffixed name
lookup), :meth:`ensure_market_data` (batch tick subscribe), and
:meth:`get_close_reason` (OPEN / SL_HIT / TP_HIT / CLOSED / UNKNOWN).

Fill discovery (cmd-19 push correlated by action id, falling back to the
cmd-12 response tickets) stays inside these methods — callers never track
action ids or poll snapshots. Rejections raise :class:`TradeError` with
retcode/symbol/action populated; ambiguous timeouts raise
:class:`MT5TimeoutError` without resending (fail closed: reconcile with
``positions_get``).
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from typing import TYPE_CHECKING, Any

from pymt5._logging import get_logger
from pymt5._parsers import (
    normalize_price,
    normalize_volume,
)
from pymt5._parsers import (
    resolve_symbol as _resolve_cached_symbol,
)
from pymt5._push_handlers import _NON_FINAL_RESULT_CODES
from pymt5.constants import (
    DEAL_ENTRY_OUT,
    DEAL_ENTRY_OUT_BY,
    ORDER_FILLING_FOK,
    ORDER_FILLING_IOC,
    ORDER_TYPE_BUY,
    ORDER_TYPE_SELL,
    POSITION_TYPE_BUY,
    TRADE_ACTION_DEAL,
    TRADE_ACTION_SLTP,
    TRADE_RETCODE_DESCRIPTIONS,
    TRADE_RETCODE_POSITION_CLOSED,
    TRADE_RETCODE_REQUOTE,
)
from pymt5.exceptions import MT5TimeoutError, SymbolNotFoundError, TradeError, ValidationError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pymt5.types import Record, RecordList, SymbolInfo, TradeResult

logger = get_logger("pymt5.high_level")

_SIDE_TO_ORDER_TYPE = {"buy": ORDER_TYPE_BUY, "sell": ORDER_TYPE_SELL}


class _PushRequote(Exception):
    """Internal: final push reports requote despite cmd-12 success.

    Carries refreshed quote fields from the push (0.0 when absent) for the
    retry path in :meth:`_place_with_fill`.
    """

    def __init__(self, bid: float = 0.0, ask: float = 0.0) -> None:
        super().__init__("requote")
        self.bid = bid
        self.ask = ask


def _push_float(push: Record, key: str) -> float:
    try:
        return float(push.get(key, 0.0) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _push_ticket(push: Record) -> int:
    """First nonzero ticket from the push, else 0 (answered without executing)."""
    for key in ("trade_position", "trade_order"):
        try:
            ticket = int(push.get(key, 0) or 0)
        except (TypeError, ValueError):
            continue
        if ticket:
            return ticket
    return 0


def _push_code(push: Record) -> int | None:
    """Explicit terminal code from the push, else None.

    Reads ``action_result_code`` first, then ``retcode``. Present-but-garbage
    counts as absent (never invent a verdict).
    """
    for key in ("action_result_code", "retcode"):
        if key in push:
            try:
                return int(push[key])
            except (TypeError, ValueError):
                return None
    return None


class _HighLevelMixin:
    """Mixin providing ticket-based trading methods for MT5WebClient."""

    if TYPE_CHECKING:
        trade_request: Callable[..., Awaitable[TradeResult]]

        async def get_positions(self) -> RecordList: ...
        async def symbol_info(self, symbol: str) -> Record | None: ...
        async def wait_for_trade_result(self, action_id: int, timeout: float = 20.0) -> Record | None: ...
        async def positions_get(
            self, symbol: str | None = ..., group: str | None = ..., ticket: int | None = ...
        ) -> RecordList: ...
        async def history_deals_get(
            self,
            date_from: Any = ...,
            date_to: Any = ...,
            *,
            group: str | None = ...,
            ticket: int | None = ...,
            position: int | None = ...,
        ) -> RecordList: ...
        async def load_symbols(self, use_gzip: bool = ...) -> dict[str, SymbolInfo]: ...
        async def subscribe_ticks(self, symbol_ids: list[int]) -> None: ...
        @staticmethod
        def _volume_to_lots(volume: float, precision: int = 8) -> int: ...

    _symbols: dict[str, SymbolInfo]
    _subscribed_ids: list[int]

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

    @staticmethod
    def _requote_lf_of(info: Record) -> float:
        """Per-symbol requote delay: the server ``trade.lf`` field, else 7s."""
        trade = info.get("trade")
        lf = trade.get("lf") if isinstance(trade, dict) else None
        try:
            value = float(lf if lf is not None else 7.0)
        except (TypeError, ValueError):
            return 7.0
        return value if value > 0 else 7.0

    def _normalize_lots(self, info: Record, volume_lots: float) -> float:
        """Round *volume_lots* to the symbol step and clamp to min/max."""
        try:
            step = float(info.get("volume_step", 0.0) or 0.0)
            vmin = float(info.get("volume_min", 0.0) or 0.0)
            vmax = float(info.get("volume_max", 0.0) or 0.0)
        except (TypeError, ValueError):
            step = vmin = vmax = 0.0
        return normalize_volume(volume_lots, min_volume=vmin, max_volume=vmax, step=step)

    @staticmethod
    def _round_price(value: float | None, digits: int) -> float:
        if value is None:
            return 0.0
        return normalize_price(value, digits=digits)

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
        else ``trade_order``). A push that arrives with zero tickets is proof
        of non-execution: with an explicit final code it raises
        :class:`TradeError` (or signals a requote retry for 10004), and with
        no usable code it raises ``TradeError(retcode=0)`` — never
        :class:`MT5TimeoutError`, which is reserved for a genuinely missing
        push. Falls back to the cmd-12 response tickets only when no push
        arrived at all. Never resends.
        """
        try:
            push = await waiter
        except (KeyError, ValueError, TypeError) as exc:
            logger.debug("fill wait failed: %s", exc)
            push = None
        if push:
            ticket = _push_ticket(push)
            if ticket:
                return ticket
            code = _push_code(push)
            if code is None or code not in _NON_FINAL_RESULT_CODES:
                if code == TRADE_RETCODE_REQUOTE:
                    raise _PushRequote(bid=_push_float(push, "bid"), ask=_push_float(push, "ask"))
                if code is None:
                    raise TradeError(
                        "server answered without executing "
                        f"(no ticket, no reason code): {symbol} action={trade_action}",
                        retcode=0,
                        symbol=symbol,
                        action=trade_action,
                    )
                desc = push.get("description") or push.get("comment") or TRADE_RETCODE_DESCRIPTIONS.get(code, "")
                raise TradeError(
                    f"server rejected order without executing: {symbol} action={trade_action} retcode={code} ({desc})",
                    retcode=code,
                    symbol=symbol,
                    action=trade_action,
                )
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
        fill_timeout: float = 30.0,
        requote_retries: int = 0,
        requote_delay: float | None = None,
        requote_lf: float = 7.0,
    ) -> int:
        if requote_retries < 0:
            raise ValidationError(f"requote_retries must be >= 0, got {requote_retries}")
        if requote_delay is not None and requote_delay < 0:
            raise ValidationError(f"requote_delay must be >= 0, got {requote_delay}")
        # Monotonic send timestamp: every attempt (and the inter-retry wait)
        # consumes from this single fill_timeout budget. Like the reference
        # client, there is no per-order timer beyond the transport watchdog —
        # expiry here fails closed with no resend.
        t0 = time.monotonic()
        deadline = t0 + fill_timeout
        price = price_order
        retries_left = int(requote_retries)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise MT5TimeoutError(
                    f"fill budget ({fill_timeout}s) exhausted for {symbol} action={trade_action}; "
                    "the request may have executed — reconcile with positions_get() "
                    "(no resend was attempted)"
                )
            action_id = random.randint(1, 2**31 - 1)
            waiter = asyncio.create_task(self.wait_for_trade_result(action_id, timeout=remaining))
            try:
                result = await self.trade_request(
                    action_id=action_id,
                    trade_action=trade_action,
                    symbol=symbol,
                    volume=volume_proto,
                    digits=digits,
                    trade_type=trade_type,
                    type_filling=filling,
                    price_order=price,
                    price_sl=price_sl,
                    price_tp=price_tp,
                    deviation=deviation,
                    comment=comment,
                    position_id=position_id,
                )
            except BaseException:
                waiter.cancel()
                raise
            if result.success:
                try:
                    return await self._resolve_fill(
                        waiter=waiter, result=result, symbol=symbol, trade_action=trade_action
                    )
                except _PushRequote as rq:
                    # The final push overrules cmd-12: server answered success
                    # but reports requote. Same retry policy as cmd-12 10004.
                    if trade_action != TRADE_ACTION_DEAL or retries_left <= 0:
                        raise TradeError(
                            f"trade rejected: {symbol} action={trade_action} retcode={TRADE_RETCODE_REQUOTE} (Requote)",
                            retcode=TRADE_RETCODE_REQUOTE,
                            symbol=symbol,
                            action=trade_action,
                        ) from None
                    retries_left -= 1
                    delay = requote_delay if requote_delay is not None else requote_lf
                    refreshed = rq.ask if trade_type == ORDER_TYPE_BUY else rq.bid
                    if refreshed > 0:
                        price = refreshed
                    logger.info(
                        "requote (10004) on %s; retrying in %.1fs at refreshed price %s (%d retries left)",
                        symbol,
                        delay,
                        price,
                        retries_left,
                    )
                    await asyncio.sleep(delay)
                    continue
            waiter.cancel()
            desc = result.description or TRADE_RETCODE_DESCRIPTIONS.get(result.retcode, "")
            if result.retcode == TRADE_RETCODE_REQUOTE and trade_action == TRADE_ACTION_DEAL and retries_left > 0:
                retries_left -= 1
                delay = requote_delay if requote_delay is not None else requote_lf
                refreshed = result.ask if trade_type == ORDER_TYPE_BUY else result.bid
                if refreshed > 0:
                    price = refreshed
                logger.info(
                    "requote (10004) on %s; retrying in %.1fs at refreshed price %s (%d retries left)",
                    symbol,
                    delay,
                    price,
                    retries_left,
                )
                await asyncio.sleep(delay)
                continue
            raise TradeError(
                f"trade rejected: {symbol} action={trade_action} retcode={result.retcode} ({desc})",
                retcode=result.retcode,
                symbol=symbol,
                action=trade_action,
            )

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
        fill_timeout: float = 30.0,
        requote_retries: int = 0,
        requote_delay: float | None = None,
    ) -> int:
        """Place a market order; return the position (deal) ticket.

        Raises :class:`TradeError` on rejection and :class:`MT5TimeoutError`
        when the fill cannot be confirmed (fail closed, never resent).

        On requote (retcode 10004) with ``requote_retries > 0``, the order is
        resent automatically after ``requote_delay`` seconds (default: the
        symbol's ``trade.lf`` field, else 7.0s) at the refreshed requote
        price. Retries share the overall ``fill_timeout`` budget, so keep it
        above ``requote_delay * (retries + 1)``. Default 0 preserves
        raise-immediately behavior.
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
            requote_retries=requote_retries,
            requote_delay=requote_delay,
            requote_lf=self._requote_lf_of(info),
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
        fill_timeout: float = 30.0,
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

    # ---- bot helpers (item 4) ----

    def resolve_symbol(self, name: str) -> SymbolInfo:
        """Resolve *name* against the symbol cache (exact, then prefix).

        Handles broker suffixes such as ``XAUUSD.sml`` or ``EURUSD+`` when the
        caller asks for the plain name. Raises :class:`SymbolNotFoundError`
        when unknown or ambiguous.
        """
        return _resolve_cached_symbol(self._symbols, name)

    async def ensure_market_data(self, symbols: list[str]) -> dict[str, int]:
        """Resolve *symbols* and subscribe tick streams for the missing ones.

        Returns requested-name → symbol-id. Already-subscribed ids are
        skipped (no resubscribe); unknown names raise
        :class:`SymbolNotFoundError` listing every miss.
        """
        if not symbols:
            return {}
        if not self._symbols:
            await self.load_symbols()
        resolved: dict[str, SymbolInfo] = {}
        missing: list[str] = []
        for name in symbols:
            try:
                resolved[name] = _resolve_cached_symbol(self._symbols, name)
            except SymbolNotFoundError:
                missing.append(name)
        if missing:
            raise SymbolNotFoundError(f"unknown symbols (call load_symbols first): {missing}")
        ids = sorted({info.symbol_id for info in resolved.values()})
        fresh = [i for i in ids if i not in set(self._subscribed_ids)]
        if fresh:
            await self.subscribe_ticks(fresh)
        return {name: info.symbol_id for name, info in resolved.items()}

    @staticmethod
    def _latest_out_deal(deals: RecordList) -> Record | None:
        outs: RecordList = []
        for deal in deals:
            try:
                is_out = int(deal.get("entry", -1) or -1) in (DEAL_ENTRY_OUT, DEAL_ENTRY_OUT_BY)
            except (TypeError, ValueError):
                continue
            if is_out:
                outs.append(deal)
        if not outs:
            return None

        def _key(deal: Record) -> tuple[int, int]:
            try:
                when = int(deal.get("time_create", 0) or 0)
            except (TypeError, ValueError):
                when = 0
            try:
                ticket = int(deal.get("deal", 0) or 0)
            except (TypeError, ValueError):
                ticket = 0
            return (when, ticket)

        return max(outs, key=_key)

    @staticmethod
    def _classify_close(deal: Record) -> str:
        """Decode SL_HIT / TP_HIT / CLOSED from a closing deal.

        Signals in order: deal comment markers (``sl``/``tp`` prefixes, as
        the terminal renders them), the MQL5 deal-reason codes
        (SL=4 / TP=5), then close-price proximity to the deal SL/TP levels.
        Anything else is a plain ``CLOSED``.
        """
        comment = str(deal.get("comment", "") or "").strip().lower().lstrip("[(")
        if comment.startswith("sl"):
            return "SL_HIT"
        if comment.startswith("tp"):
            return "TP_HIT"
        try:
            reason = int(deal.get("trade_reason", -1))
        except (TypeError, ValueError):
            reason = -1
        if reason == 4:
            return "SL_HIT"
        if reason == 5:
            return "TP_HIT"
        close = 0.0
        for price_key in ("price_close", "price", "price_open"):
            try:
                close = float(deal.get(price_key, 0.0) or 0.0)
            except (TypeError, ValueError):
                continue
            if close:
                break
        if close:
            for level_key, hit in (("sl", "SL_HIT"), ("tp", "TP_HIT")):
                try:
                    level = float(deal.get(level_key, 0.0) or 0.0)
                except (TypeError, ValueError):
                    continue
                if level > 0 and math.isclose(close, level, rel_tol=1e-9):
                    return hit
        return "CLOSED"

    async def get_close_reason(self, position_id: int, *, lookback_days: float = 90.0) -> tuple[str, Record | None]:
        """Explain why a position closed: OPEN / SL_HIT / TP_HIT / CLOSED / UNKNOWN.

        Returns ``(reason, closing_deal)``; the deal is None for OPEN and
        UNKNOWN. Closed positions are found via position-scoped deal lookup,
        with an explicit time-range retry when the default history window is
        empty.
        """
        try:
            ticket = int(position_id)
        except (TypeError, ValueError):
            raise ValidationError(f"position_id must be an int, got {position_id!r}") from None
        if await self.positions_get(ticket=ticket):
            return ("OPEN", None)
        closing = self._latest_out_deal(await self.history_deals_get(position=ticket))
        if closing is None and lookback_days > 0:
            now = int(time.time())
            start = now - int(float(lookback_days) * 86400)
            closing = self._latest_out_deal(await self.history_deals_get(start, now, position=ticket))
        if closing is None:
            return ("UNKNOWN", None)
        return (self._classify_close(closing), closing)
