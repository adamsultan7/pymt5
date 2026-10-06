"""Blocking client for trading-bot authors.

:class:`SyncMT5Client` wraps :class:`pymt5.client.MT5WebClient` with a private
background event-loop thread, so bot code stays fully synchronous — no
``asyncio`` in user code::

    from pymt5.sync import SyncMT5Client

    with SyncMT5Client() as client:
        client.login(12345, "password")
        client.load_symbols()
        client.subscribe_ticks(client.subscribe_symbols(["EURUSD"]))
        ticket = client.place_market("EURUSD", "buy", 0.01)  # item 3 API

Rules:

- every network call blocks with a per-call timeout (``timeout`` keyword,
  defaulting to the client timeout) and raises loudly — connection problems
  raise :class:`MT5ConnectionError`, timeouts raise :class:`MT5TimeoutError`,
  rejections raise :class:`TradeError`. Nothing returns silent empties.
- push-handler callbacks (``on_tick`` et al.) run on the background loop
  thread: keep them fast, non-blocking, and thread-safe. Never call blocking
  ``SyncMT5Client`` methods from inside a push callback (it would deadlock).
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine
from datetime import datetime
from typing import Any, TypeVar

from pymt5._logging import get_logger
from pymt5._metrics import MetricsCollector
from pymt5.client import MT5WebClient
from pymt5.constants import COPY_TICKS_ALL, DEFAULT_WS_URI
from pymt5.events import ConnectionStats, HealthStatus
from pymt5.exceptions import MT5ConnectionError, MT5TimeoutError
from pymt5.transport import CommandResult
from pymt5.types import AccountInfo, Record, RecordList, SymbolInfo, TradeResult

logger = get_logger("pymt5.sync")

_T = TypeVar("_T")


class SyncMT5Client:
    """Synchronous facade over :class:`MT5WebClient`."""

    def __init__(
        self,
        uri: str = DEFAULT_WS_URI,
        timeout: float = 30.0,
        heartbeat_interval: float = 5.0,
        tick_history_limit: int = 10000,
        max_tick_symbols: int = 0,
        auto_reconnect: bool = False,
        max_reconnect_attempts: int | None = 5,
        reconnect_delay: float = 3.0,
        max_reconnect_delay: float = 60.0,
        rate_limit: float = 0,
        rate_burst: int = 20,
        metrics: MetricsCollector | None = None,
        symbol_cache_ttl: float = 0,
        ws_ping_interval: float | None = None,
        ws_ping_timeout: float | None = None,
        heartbeat_failure_threshold: int = 3,
        heartbeat_stale_after: float = 15.0,
    ) -> None:
        self._timeout = timeout
        self._client = MT5WebClient(
            uri=uri,
            timeout=timeout,
            heartbeat_interval=heartbeat_interval,
            tick_history_limit=tick_history_limit,
            max_tick_symbols=max_tick_symbols,
            auto_reconnect=auto_reconnect,
            max_reconnect_attempts=max_reconnect_attempts,
            reconnect_delay=reconnect_delay,
            max_reconnect_delay=max_reconnect_delay,
            rate_limit=rate_limit,
            rate_burst=rate_burst,
            metrics=metrics,
            symbol_cache_ttl=symbol_cache_ttl,
            ws_ping_interval=ws_ping_interval,
            ws_ping_timeout=ws_ping_timeout,
            heartbeat_failure_threshold=heartbeat_failure_threshold,
            heartbeat_stale_after=heartbeat_stale_after,
        )
        self._connect_lock = threading.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._start_loop()

    # ---- loop lifecycle ----

    def _start_loop(self) -> None:
        loop = asyncio.new_event_loop()
        ready = threading.Event()

        def _run() -> None:
            asyncio.set_event_loop(loop)
            ready.set()
            loop.run_forever()

        thread = threading.Thread(target=_run, name="pymt5-sync-loop", daemon=True)
        thread.start()
        ready.wait()
        self._loop = loop
        self._thread = thread

    def _call(self, coro: Coroutine[Any, Any, _T], timeout: float | None) -> _T:
        """Run *coro* on the background loop and return its result loudly."""
        loop = self._loop
        if loop is None:
            try:
                coro.close()
            except Exception:
                pass
            raise MT5ConnectionError("sync client is closed")
        limit = self._timeout if timeout is None else timeout
        try:
            future = asyncio.run_coroutine_threadsafe(coro, loop)
        except RuntimeError as exc:
            raise MT5ConnectionError(f"sync client loop is not running: {exc}") from exc
        try:
            return future.result(timeout=limit)
        except MT5TimeoutError:
            raise
        except TimeoutError as exc:
            future.cancel()
            raise MT5TimeoutError(f"sync call timed out after {limit}s") from exc

    @property
    def async_client(self) -> MT5WebClient:
        """Escape hatch to the wrapped async client for advanced use."""
        return self._client

    @property
    def is_connected(self) -> bool:
        return self._client.is_connected

    @property
    def server_build(self) -> int:
        return self._client.server_build

    # ---- lifecycle ----

    def connect(self, timeout: float | None = None) -> SyncMT5Client:
        """Connect (single-flight across threads); no-op when already ready."""
        with self._connect_lock:
            if self._client.transport.is_ready:
                return self
            self._call(self._client.connect(), timeout)
            return self

    def close(self, timeout: float | None = None) -> None:
        """Close the session and stop the background loop. Idempotent."""
        with self._connect_lock:
            loop, self._loop = self._loop, None
            thread, self._thread = self._thread, None
            if loop is None:
                return
            try:
                future = asyncio.run_coroutine_threadsafe(self._client.close(), loop)
                future.result(timeout=self._timeout if timeout is None else timeout)
            finally:
                loop.call_soon_threadsafe(loop.stop)
                if thread is not None:
                    thread.join(timeout=self._timeout if timeout is None else timeout)
                    if thread.is_alive():
                        logger.warning("sync loop thread did not stop in time")

    def __enter__(self) -> SyncMT5Client:
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

    # ---- session ----

    def login(
        self,
        login: int,
        password: str,
        url: str = "",
        session: int = 0,
        otp: str = "",
        version: int = 0,
        cid: bytes | None = None,
        lead_cookie_id: int = 0,
        lead_affiliate_site: str = "",
        utm_campaign: str = "",
        utm_source: str = "",
        auto_heartbeat: bool = True,
        timeout: float | None = None,
    ) -> tuple[str, int]:
        return self._call(
            self._client.login(
                login=login,
                password=password,
                url=url,
                session=session,
                otp=otp,
                version=version,
                cid=cid,
                lead_cookie_id=lead_cookie_id,
                lead_affiliate_site=lead_affiliate_site,
                utm_campaign=utm_campaign,
                utm_source=utm_source,
                auto_heartbeat=auto_heartbeat,
            ),
            timeout,
        )

    def logout(self, timeout: float | None = None) -> None:
        self._call(self._client.logout(), timeout)

    def ping(self, timeout: float | None = None) -> None:
        self._call(self._client.ping(), timeout)

    def last_error(self) -> tuple[int, str]:
        return self._client.last_error()

    # ---- symbols / market data ----

    def load_symbols(self, use_gzip: bool = True, timeout: float | None = None) -> dict[str, SymbolInfo]:
        return self._call(self._client.load_symbols(use_gzip=use_gzip), timeout)

    def get_symbols(self, use_gzip: bool = True, timeout: float | None = None) -> RecordList:
        return self._call(self._client.get_symbols(use_gzip=use_gzip), timeout)

    def symbols_get(self, group: str | None = None, use_gzip: bool = True, timeout: float | None = None) -> RecordList:
        return self._call(self._client.symbols_get(group=group, use_gzip=use_gzip), timeout)

    def get_full_symbol_info(self, symbol: str, timeout: float | None = None) -> Record | None:
        return self._call(self._client.get_full_symbol_info(symbol), timeout)

    def symbol_info(self, symbol: str, timeout: float | None = None) -> Record | None:
        return self._call(self._client.symbol_info(symbol), timeout)

    def get_symbol_info(self, name: str) -> SymbolInfo | None:
        return self._client.get_symbol_info(name)

    def get_symbol_id(self, name: str) -> int | None:
        return self._client.get_symbol_id(name)

    @property
    def symbol_names(self) -> list[str]:
        return self._client.symbol_names

    def symbol_select(self, symbol: str, enable: bool = True, timeout: float | None = None) -> bool:
        return self._call(self._client.symbol_select(symbol, enable), timeout)

    def get_symbol_groups(self, timeout: float | None = None) -> list[str]:
        return self._call(self._client.get_symbol_groups(), timeout)

    def get_spreads(self, symbol_ids: list[int] | None = None, timeout: float | None = None) -> RecordList:
        return self._call(self._client.get_spreads(symbol_ids), timeout)

    def subscribe_ticks(self, symbol_ids: list[int], timeout: float | None = None) -> None:
        self._call(self._client.subscribe_ticks(symbol_ids), timeout)

    def unsubscribe_ticks(self, symbol_ids: list[int], timeout: float | None = None) -> None:
        self._call(self._client.unsubscribe_ticks(symbol_ids), timeout)

    def subscribe_symbols(self, symbol_names: list[str], timeout: float | None = None) -> list[int]:
        return self._call(self._client.subscribe_symbols(symbol_names), timeout)

    def subscribe_book(self, symbol_ids: list[int], timeout: float | None = None) -> None:
        self._call(self._client.subscribe_book(symbol_ids), timeout)

    def unsubscribe_book(self, symbol_ids: list[int], timeout: float | None = None) -> None:
        self._call(self._client.unsubscribe_book(symbol_ids), timeout)

    def subscribe_book_by_name(self, symbol_names: list[str], timeout: float | None = None) -> list[int]:
        return self._call(self._client.subscribe_book_by_name(symbol_names), timeout)

    def market_book_add(self, symbol: str, timeout: float | None = None) -> bool:
        return self._call(self._client.market_book_add(symbol), timeout)

    def market_book_release(self, symbol: str, timeout: float | None = None) -> bool:
        return self._call(self._client.market_book_release(symbol), timeout)

    def market_book_get(self, symbol: str) -> Record | None:
        return self._client.market_book_get(symbol)

    def symbol_info_tick(self, symbol: str) -> Record | None:
        return self._client.symbol_info_tick(symbol)

    def get_rates(
        self, symbol: str, period_minutes: int, from_ts: int, to_ts: int, timeout: float | None = None
    ) -> RecordList:
        return self._call(self._client.get_rates(symbol, period_minutes, from_ts, to_ts), timeout)

    def copy_rates_range(
        self,
        symbol: str,
        timeframe: int,
        date_from: int | float | datetime,
        date_to: int | float | datetime,
        timeout: float | None = None,
    ) -> RecordList:
        return self._call(self._client.copy_rates_range(symbol, timeframe, date_from, date_to), timeout)

    def copy_rates_from(
        self,
        symbol: str,
        timeframe: int,
        date_from: int | float | datetime,
        count: int,
        timeout: float | None = None,
    ) -> RecordList:
        return self._call(self._client.copy_rates_from(symbol, timeframe, date_from, count), timeout)

    def copy_rates_from_pos(
        self, symbol: str, timeframe: int, start_pos: int, count: int, timeout: float | None = None
    ) -> RecordList:
        return self._call(self._client.copy_rates_from_pos(symbol, timeframe, start_pos, count), timeout)

    def copy_ticks_from(
        self,
        symbol: str,
        date_from: int | float | datetime,
        count: int,
        flags: int = COPY_TICKS_ALL,
        timeout: float | None = None,
    ) -> RecordList:
        return self._call(self._client.copy_ticks_from(symbol, date_from, count, flags), timeout)

    def copy_ticks_range(
        self,
        symbol: str,
        date_from: int | float | datetime,
        date_to: int | float | datetime,
        flags: int = COPY_TICKS_ALL,
        timeout: float | None = None,
    ) -> RecordList:
        return self._call(self._client.copy_ticks_range(symbol, date_from, date_to, flags), timeout)

    # ---- account ----

    def get_account(self, timeout: float | None = None) -> Record:
        return self._call(self._client.get_account(), timeout)

    def account_info(self, timeout: float | None = None) -> Record:
        return self._call(self._client.account_info(), timeout)

    def get_account_summary(self, timeout: float | None = None) -> AccountInfo:
        return self._call(self._client.get_account_summary(), timeout)

    # ---- positions / orders / history ----

    def get_positions_and_orders(self, timeout: float | None = None) -> dict[str, RecordList]:
        return self._call(self._client.get_positions_and_orders(), timeout)

    def get_positions(self, timeout: float | None = None) -> RecordList:
        return self._call(self._client.get_positions(), timeout)

    def positions_get(
        self,
        symbol: str | None = None,
        group: str | None = None,
        ticket: int | None = None,
        timeout: float | None = None,
    ) -> RecordList:
        return self._call(self._client.positions_get(symbol=symbol, group=group, ticket=ticket), timeout)

    def get_orders(self, timeout: float | None = None) -> RecordList:
        return self._call(self._client.get_orders(), timeout)

    def orders_get(
        self,
        symbol: str | None = None,
        group: str | None = None,
        ticket: int | None = None,
        timeout: float | None = None,
    ) -> RecordList:
        return self._call(self._client.orders_get(symbol=symbol, group=group, ticket=ticket), timeout)

    def get_trade_history(
        self, from_ts: int = 0, to_ts: int = 0, timeout: float | None = None
    ) -> dict[str, RecordList]:
        return self._call(self._client.get_trade_history(from_ts, to_ts), timeout)

    def get_deals(self, from_ts: int = 0, to_ts: int = 0, timeout: float | None = None) -> RecordList:
        return self._call(self._client.get_deals(from_ts, to_ts), timeout)

    def history_orders_get(
        self,
        date_from: int | float | datetime | None = None,
        date_to: int | float | datetime | None = None,
        group: str | None = None,
        ticket: int | None = None,
        position: int | None = None,
        timeout: float | None = None,
    ) -> RecordList:
        return self._call(
            self._client.history_orders_get(date_from, date_to, group=group, ticket=ticket, position=position),
            timeout,
        )

    def history_deals_get(
        self,
        date_from: int | float | datetime | None = None,
        date_to: int | float | datetime | None = None,
        group: str | None = None,
        ticket: int | None = None,
        position: int | None = None,
        timeout: float | None = None,
    ) -> RecordList:
        return self._call(
            self._client.history_deals_get(date_from, date_to, group=group, ticket=ticket, position=position),
            timeout,
        )

    # ---- trading ----

    def trade_request(
        self,
        *,
        timeout: float | None = None,
        **kwargs: Any,
    ) -> TradeResult:
        return self._call(self._client.trade_request(**kwargs), timeout)

    def order_send(self, request: Record, timeout: float | None = None) -> TradeResult:
        return self._call(self._client.order_send(request), timeout)

    def buy_market(
        self,
        symbol: str,
        volume: float,
        *,
        sl: float = 0.0,
        tp: float = 0.0,
        deviation: int = 20,
        comment: str = "",
        timeout: float | None = None,
    ) -> TradeResult:
        return self._call(
            self._client.buy_market(symbol, volume, sl=sl, tp=tp, deviation=deviation, comment=comment), timeout
        )

    def sell_market(
        self,
        symbol: str,
        volume: float,
        *,
        sl: float = 0.0,
        tp: float = 0.0,
        deviation: int = 20,
        comment: str = "",
        timeout: float | None = None,
    ) -> TradeResult:
        return self._call(
            self._client.sell_market(symbol, volume, sl=sl, tp=tp, deviation=deviation, comment=comment), timeout
        )

    def buy_limit(
        self, symbol: str, volume: float, price: float, timeout: float | None = None, **kwargs: Any
    ) -> TradeResult:
        return self._call(self._client.buy_limit(symbol, volume, price, **kwargs), timeout)

    def sell_limit(
        self, symbol: str, volume: float, price: float, timeout: float | None = None, **kwargs: Any
    ) -> TradeResult:
        return self._call(self._client.sell_limit(symbol, volume, price, **kwargs), timeout)

    def buy_stop(
        self, symbol: str, volume: float, price: float, timeout: float | None = None, **kwargs: Any
    ) -> TradeResult:
        return self._call(self._client.buy_stop(symbol, volume, price, **kwargs), timeout)

    def sell_stop(
        self, symbol: str, volume: float, price: float, timeout: float | None = None, **kwargs: Any
    ) -> TradeResult:
        return self._call(self._client.sell_stop(symbol, volume, price, **kwargs), timeout)

    def close_position(
        self, symbol: str, position_id: int, volume: float, timeout: float | None = None, **kwargs: Any
    ) -> TradeResult:
        return self._call(self._client.close_position(symbol, position_id, volume, **kwargs), timeout)

    def close_position_by(
        self, symbol: str, position_id: int, position_by: int, timeout: float | None = None
    ) -> TradeResult:
        return self._call(self._client.close_position_by(symbol, position_id, position_by), timeout)

    def modify_position_sltp(
        self, symbol: str, position_id: int, sl: float = 0.0, tp: float = 0.0, timeout: float | None = None
    ) -> TradeResult:
        return self._call(self._client.modify_position_sltp(symbol, position_id, sl, tp), timeout)

    def modify_pending_order(
        self, symbol: str, order: int, price: float, timeout: float | None = None, **kwargs: Any
    ) -> TradeResult:
        return self._call(self._client.modify_pending_order(symbol, order, price, **kwargs), timeout)

    def cancel_pending_order(self, order: int, timeout: float | None = None) -> TradeResult:
        return self._call(self._client.cancel_pending_order(order), timeout)

    def place_market(
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
        timeout: float | None = None,
    ) -> int:
        """Place a market order; return the position ticket (see async API)."""
        return self._call(
            self._client.place_market(
                symbol,
                side,
                volume_lots,
                sl=sl,
                tp=tp,
                deviation=deviation,
                comment=comment,
                filling=filling,
                fill_timeout=fill_timeout,
                requote_retries=requote_retries,
                requote_delay=requote_delay,
            ),
            timeout,
        )

    def close_position_by_ticket(
        self,
        ticket: int,
        *,
        deviation: int = 20,
        comment: str = "",
        volume_lots: float | None = None,
        fill_timeout: float = 30.0,
        timeout: float | None = None,
    ) -> int:
        """Close an open position by ticket; return the closing deal ticket."""
        return self._call(
            self._client.close_position_by_ticket(
                ticket,
                deviation=deviation,
                comment=comment,
                volume_lots=volume_lots,
                fill_timeout=fill_timeout,
            ),
            timeout,
        )

    def modify_sltp(self, ticket: int, sl: float, tp: float, timeout: float | None = None) -> TradeResult:
        """Move SL/TP of an open position by ticket."""
        return self._call(self._client.modify_sltp(ticket, sl, tp), timeout)

    def resolve_symbol(self, name: str) -> SymbolInfo:
        """Resolve a symbol name against the cache (exact, then prefix)."""
        return self._client.resolve_symbol(name)

    def ensure_market_data(self, symbols: list[str], timeout: float | None = None) -> dict[str, int]:
        """Subscribe tick streams for *symbols*; return name → symbol id."""
        return self._call(self._client.ensure_market_data(symbols), timeout)

    def get_close_reason(
        self, position_id: int, lookback_days: float = 90.0, timeout: float | None = None
    ) -> tuple[str, Record | None]:
        """Explain why a position closed; see the async API."""
        return self._call(self._client.get_close_reason(position_id, lookback_days=lookback_days), timeout)

    def wait_for_trade_result(self, action_id: int, timeout: float = 20.0) -> Record | None:
        """Block until the cmd-19 push for *action_id* arrives (None on timeout)."""
        call_timeout = timeout + self._timeout if timeout > 0 else self._timeout
        return self._call(self._client.wait_for_trade_result(action_id, timeout), call_timeout)

    def send_raw_command(
        self, command: int, payload: bytes | None = None, timeout: float | None = None
    ) -> CommandResult:
        return self._call(self._client.send_raw_command(command, payload), timeout)

    def health_check(self, timeout: float | None = None) -> HealthStatus:
        return self._call(self._client.health_check(), timeout)

    # ---- handler registration (synchronous, thread-safe enough) ----

    def on_tick(self, callback: Callable) -> Callable:
        return self._client.on_tick(callback)

    def on_book_update(self, callback: Callable) -> Callable:
        return self._client.on_book_update(callback)

    def on_trade_update(self, callback: Callable) -> Callable:
        return self._client.on_trade_update(callback)

    def on_position_update(self, callback: Callable) -> Callable:
        return self._client.on_position_update(callback)

    def on_order_update(self, callback: Callable) -> Callable:
        return self._client.on_order_update(callback)

    def on_trade_result(self, callback: Callable) -> Callable:
        return self._client.on_trade_result(callback)

    def on_tick_event(self, callback: Callable) -> Callable:
        return self._client.on_tick_event(callback)

    def on_book_event(self, callback: Callable) -> Callable:
        return self._client.on_book_event(callback)

    def on_trade_result_event(self, callback: Callable) -> Callable:
        return self._client.on_trade_result_event(callback)

    def on_account_event(self, callback: Callable) -> Callable:
        return self._client.on_account_event(callback)

    def on_disconnect(self, callback: Callable[[], None]) -> None:
        self._client.on_disconnect(callback)

    def connection_stats(self) -> ConnectionStats:
        """Return cumulative connection counters (local reads, never blocks)."""
        return self._client.connection_stats()
