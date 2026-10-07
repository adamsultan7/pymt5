import asyncio
import contextlib
import os
import random
import time
from collections import deque
from collections.abc import Callable
from typing import Any, TypeVar

# Import mixins
from pymt5._account import _AccountMixin
from pymt5._high_level import _HighLevelMixin
from pymt5._logging import get_logger
from pymt5._market_data import _MarketDataMixin
from pymt5._metrics import MetricsCollector
from pymt5._order_helpers import _OrderHelpersMixin

# Import parser functions (with re-exports for backward compatibility)
from pymt5._parsers import (  # noqa: F401  — re-exported for backward compatibility
    BUY_ORDER_TYPES,
    PERIOD_MINUTES_MAP,
    SELL_ORDER_TYPES,
    _coerce_optional_timestamp,
    _coerce_timestamp,
    _coerce_timestamp_ms,
    _coerce_timestamp_ms_end,
    _currencies_equal,
    _history_lookback_seconds,
    _matches_group_mask,
    _normalize_full_symbol_record,
    _normalize_timeframe_minutes,
    _order_side,
    _parse_account_response,
    _parse_book_entries,
    _parse_counted_records,
    _parse_open_account_result,
    _parse_rate_bars,
    _parse_tick_batch,
    _parse_verification_status,
    _tick_matches_copy_flags,
    _to_copy_tick_record,
    _validate_requested_stops,
    _validate_requested_volume,
)
from pymt5._push_handlers import _PushHandlersMixin
from pymt5._trading import _TradingMixin
from pymt5.constants import (
    CMD_BOOK_PUSH,
    CMD_INIT,
    CMD_LOGIN,
    CMD_LOGOUT,
    CMD_PING,
    CMD_TICK_PUSH,
    DEFAULT_WS_URI,
    PROP_BYTES,
    PROP_FIXED_STRING,
    PROP_U32,
    PROP_U64,
)
from pymt5.events import ConnectionStats, HealthStatus
from pymt5.exceptions import MT5TimeoutError, SessionError, ValidationError
from pymt5.helpers import build_client_id, bytes_to_hex
from pymt5.protocol import SeriesCodec
from pymt5.transport import CommandResult, MT5WebSocketTransport

# Import types (with re-exports for backward compatibility)
from pymt5.types import (  # noqa: F401  — re-exported for backward compatibility
    LOGIN_RESPONSE_SCHEMA,
    OPEN_ACCOUNT_RESPONSE_SCHEMA,
    REAL_ACCOUNT_RESERVED_PAYLOAD,
    TRADE_RESPONSE_SCHEMA,
    TRADER_PARAMS_SCHEMA,
    VERIFICATION_STATUS_SCHEMA,
    AccountDocument,
    AccountInfo,
    AccountOpeningRequest,
    DemoAccountRequest,
    OpenAccountResult,
    RealAccountRequest,
    Record,
    RecordList,
    SymbolInfo,
    TradeResult,
    VerificationStatus,
)

logger = get_logger("pymt5.client")

_T = TypeVar("_T")
MT5_TERMINAL_VERSION = 500
OBSERVED_WEBTERMINAL_BUILD_RELEASE_DATES = {
    5687: "15 Mar 2026",
}


class MT5WebClient(
    _PushHandlersMixin, _AccountMixin, _MarketDataMixin, _TradingMixin, _OrderHelpersMixin, _HighLevelMixin
):
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
    ):
        self.uri = uri
        self.timeout = timeout
        self._rate_limit = rate_limit
        self._rate_burst = rate_burst
        self._metrics = metrics
        self._ws_ping_interval = ws_ping_interval
        self._ws_ping_timeout = ws_ping_timeout
        self._heartbeat_failure_threshold = max(1, int(heartbeat_failure_threshold))
        self._heartbeat_stale_after = max(0.0, float(heartbeat_stale_after))
        self._heartbeat_failures = 0
        self._last_heartbeat_ok: float = 0.0
        self.transport = MT5WebSocketTransport(
            uri=uri,
            timeout=timeout,
            rate_limit=rate_limit,
            rate_burst=rate_burst,
            metrics=metrics,
            ws_ping_interval=ws_ping_interval,
            ws_ping_timeout=ws_ping_timeout,
        )
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_task: asyncio.Task | None = None
        self._symbols: dict[str, SymbolInfo] = {}
        self._symbols_by_id: dict[int, SymbolInfo] = {}
        self._full_symbols: dict[str, Record] = {}
        # Per-symbol working fill mode: last type_filling that filled per
        # symbol, consulted before the spec lookup (brokers that omit fill
        # flags can never auto-detect). Lifetime = client lifetime; a later
        # 10030 re-triggers the alternate-mode retry and re-records.
        self._fill_mode_memory: dict[str, int] = {}
        self._fills_resolved: int = 0
        self._fills_first_try: int = 0
        self._fill_fallback_sends: int = 0
        self._symbol_cache_ttl: float = max(0.0, float(symbol_cache_ttl))
        self._symbols_loaded_at: float = 0.0
        self._tick_cache_by_id: dict[int, Record] = {}
        self._tick_cache_by_name: dict[str, Record] = {}
        self._tick_history_limit = max(0, int(tick_history_limit))
        self._max_tick_symbols = max(0, int(max_tick_symbols))
        self._tick_history_by_id: dict[int, deque[Record]] = {}
        self._tick_history_by_name: dict[str, deque[Record]] = {}
        self._tick_history_access_order: list[int] = []
        self._book_cache_by_id: dict[int, Record] = {}
        self._book_cache_by_name: dict[str, Record] = {}
        self._last_error: tuple[int, str] = (0, "")
        self._logged_in = False
        self._bootstrap_pristine = False
        # Reconnect settings
        self._auto_reconnect = auto_reconnect
        self._max_reconnect_attempts = max_reconnect_attempts
        self._reconnect_delay = reconnect_delay
        self._max_reconnect_delay = max_reconnect_delay
        self._reconnect_task: asyncio.Task | None = None
        self._closing = False
        # Serializes explicit connect() against reconnect attempts so two
        # transports are never built concurrently (last-writer-wins orphans
        # fed reconnect cascades). Survives transport replacement, unlike
        # the per-object transport lock.
        self._conn_lock = asyncio.Lock()
        # Bumped on every transport replacement; an attempt that finds a
        # different generation under it aborts quietly instead of tearing
        # down a session it did not build.
        self._transport_generation = 0
        # Flap episode (Bug C): open from the first disconnect until a
        # reconnect succeeds or the client closes. Attempt/success lines
        # collapse into one summary; only the first disconnect logs loud.
        self._flap: dict[str, Any] | None = None
        # Stored credentials for reconnect
        self._login_kwargs: dict | None = None
        self._subscribed_ids: list[int] = []
        self._subscribed_book_ids: list[int] = []
        # User disconnect callback
        self._on_disconnect: Callable[[], None] | None = None
        # Typed push handler callback lists (Phase 16.1)
        self._typed_tick_handlers: list[Callable] = []
        self._typed_book_handlers: list[Callable] = []
        self._typed_trade_result_handlers: list[Callable] = []
        self._typed_account_handlers: list[Callable] = []
        # Callback error handlers (Phase 16.2)
        self._callback_error_handlers: list[Callable] = []
        # Connection health monitoring (Phase 16.3)
        self._reconnect_count: int = 0
        self._connected_at: float = 0.0
        # Cumulative observability counters (connection_stats)
        self._connect_count: int = 0
        self._disconnect_count: int = 0
        self._heartbeat_failure_total: int = 0
        self._last_connection_error: str | None = None
        self._health_degraded_callbacks: list[Callable] = []
        self._health_degraded_threshold_ms: float = 5000.0
        self.transport.on(CMD_TICK_PUSH, self._cache_tick_push)
        self.transport.on(CMD_BOOK_PUSH, self._cache_book_push)
        # Wire up transport disconnect handler
        self.transport._on_disconnect = self._handle_disconnect
        self.transport._on_demand_recover = self._recover_transport_on_demand

    @property
    def is_connected(self) -> bool:
        return self.transport.is_ready and self._logged_in

    @property
    def server_build(self) -> int:
        """Server build number extracted from the bootstrap handshake."""
        return self.transport.server_build

    def on_disconnect(self, callback: Callable[[], None]) -> None:
        """Register a callback for disconnect events."""
        self._on_disconnect = callback

    async def connect(self) -> "MT5WebClient":
        # If a reconnect round is already flying, join it (bounded by the
        # command timeout) instead of building a second transport beside it.
        task = self._reconnect_task
        if task is not None and not task.done():
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=self.timeout)
            except TimeoutError:
                logger.debug("connect: reconnect still in flight; proceeding under lock")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.debug("connect: in-flight reconnect failed; connecting directly", exc_info=True)
        async with self._conn_lock:
            if self.transport.is_ready:
                logger.debug("connect: transport already ready; reusing session")
                return self
            await self.transport.connect()
            self._bootstrap_pristine = True
            self._connected_at = time.monotonic()
            self._heartbeat_failures = 0
            self._last_heartbeat_ok = 0.0
            self._connect_count += 1
            if self.transport.server_build:
                logger.info("connected to %s (server_build=%d)", self.uri, self.transport.server_build)
            else:
                logger.info("connected to %s", self.uri)
            return self

    async def initialize(
        self,
        *,
        version: int = 0,
        password: str = "",
        otp: str = "",
        cid: bytes | None = None,
    ) -> CommandResult:
        """Official-style alias for cmd=29 session initialization."""
        if not self.transport.is_ready:
            await self.connect()
        return await self.init_session(version=version, password=password, otp=otp, cid=cid)

    async def close(self) -> None:
        self._closing = True
        self._stop_heartbeat()
        if self._reconnect_task is not None:
            task, self._reconnect_task = self._reconnect_task, None
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        if self._logged_in:
            try:
                await self.logout()
            except Exception:
                logger.debug("logout during close() failed", exc_info=True)
            self._logged_in = False
        await self.transport.close()
        self._bootstrap_pristine = False
        self._closing = False
        self._flap = None
        # Clear stored credentials from memory
        self._clear_credentials()
        logger.info("connection closed")

    def _clear_credentials(self) -> None:
        """Clear stored credentials from memory for security."""
        if self._login_kwargs is not None:
            # Zero-fill password before discarding the reference
            pw = self._login_kwargs.get("password")
            if isinstance(pw, str) and pw:
                self._login_kwargs["password"] = "\x00" * len(pw)
            self._login_kwargs = None
            logger.debug("credentials cleared from memory")

    async def shutdown(self) -> None:
        """Official-style alias for closing the websocket session."""
        await self.close()

    def last_error(self) -> tuple[int, str]:
        """Return the latest client-side compatibility-layer error."""
        return self._last_error

    async def __aenter__(self) -> "MT5WebClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()

    # ---- Heartbeat ----

    def _start_heartbeat(self) -> None:
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            return
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    def _stop_heartbeat(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        # Official-UI semantics: ping every interval; the session is dead
        # when no ping has SUCCEEDED for longer than heartbeat_stale_after.
        # Each ping is bounded by the interval so a hung ping counts toward
        # the window instead of stalling detection. The consecutive-failure
        # threshold below stays as a second, independent tripwire.
        try:
            while True:
                await asyncio.sleep(self._heartbeat_interval)
                try:
                    await asyncio.wait_for(self.ping(), timeout=self._heartbeat_interval)
                    self._heartbeat_failures = 0
                    self._last_heartbeat_ok = time.monotonic()
                    logger.debug("heartbeat ping ok")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._heartbeat_failures += 1
                    self._heartbeat_failure_total += 1
                    logger.warning(
                        "heartbeat ping failed (%d/%d): %s",
                        self._heartbeat_failures,
                        self._heartbeat_failure_threshold,
                        exc,
                    )
                    if self._heartbeat_failures >= self._heartbeat_failure_threshold:
                        logger.error(
                            "heartbeat failed %d times in a row; treating as disconnect",
                            self._heartbeat_failures,
                        )
                        await self._heartbeat_dead(exc)
                        # Exit the loop; _handle_disconnect() already stopped
                        # the task reference and scheduled a reconnect when enabled.
                        return
                if self._last_heartbeat_ok > 0:
                    silent_for = time.monotonic() - self._last_heartbeat_ok
                    if silent_for > self._heartbeat_stale_after:
                        logger.error(
                            "no successful heartbeat for %.1fs (limit %.1fs); treating as disconnect",
                            silent_for,
                            self._heartbeat_stale_after,
                        )
                        await self._heartbeat_dead(
                            MT5TimeoutError(
                                f"no successful heartbeat for {silent_for:.1f}s "
                                f"(limit {self._heartbeat_stale_after:.1f}s)"
                            )
                        )
                        return
        except asyncio.CancelledError:
            pass

    async def _heartbeat_dead(self, exc: BaseException) -> None:
        """Route a dead heartbeat through the normal disconnect path."""
        try:
            await self.transport._handle_connection_loss(exc)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("transport disconnect handling failed", exc_info=True)

    # ---- Reconnect ----

    def _schedule_reconnect(self) -> asyncio.Task | None:
        """Start one reconnect round unless one is already running.

        Returns the running task, or None when reconnect is disabled,
        the client is closing, or no credentials are stored.
        """
        if self._auto_reconnect and not self._closing and self._login_kwargs:
            task = self._reconnect_task
            if task is not None and not task.done():
                logger.debug("reconnect already in progress, skipping")
                return task
            task = asyncio.create_task(self._reconnect_loop())
            self._reconnect_task = task
            return task
        return None

    def _handle_disconnect(self) -> None:
        """Called by transport when the WebSocket disconnects unexpectedly."""
        self._logged_in = False
        self._bootstrap_pristine = False
        self._stop_heartbeat()
        self._disconnect_count += 1
        self._last_connection_error = self.transport.last_disconnect_reason
        if self._flap is None:
            self._flap = {
                "start": time.monotonic(),
                "attempts": 0,
                "last_error": self._last_connection_error,
            }
            logger.warning("disconnected from server")
        else:
            logger.debug(
                "disconnect during ongoing episode (%.1fs in)",
                time.monotonic() - self._flap["start"],
            )
        if self._on_disconnect:
            try:
                self._on_disconnect()
            except Exception:
                logger.warning("on_disconnect handler raised", exc_info=True)
        self._schedule_reconnect()

    async def _recover_transport_on_demand(self) -> MT5WebSocketTransport | None:
        """Run one reconnect round when a call finds the transport in ERROR.

        This is the lazy self-heal for an exhausted reconnect loop: the next
        public call after a long outage triggers a fresh reconnect round and,
        on success, is re-dispatched on the live transport instead of raising
        forever. Returns the live transport, or None when recovery is not
        possible (disabled, closing, no credentials) or the round failed.
        """
        task = self._schedule_reconnect()
        if task is None or task is asyncio.current_task():
            # No round to join, or this IS the round (a send inside the
            # reconnect body must not await itself — let the round handle it).
            return None
        try:
            await task
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("on-demand reconnect round failed", exc_info=True)
        live = self.transport
        if live.is_ready and self._logged_in:
            return live
        return None

    def _migrate_transport_listeners(self, old: MT5WebSocketTransport, new: MT5WebSocketTransport) -> None:
        """Carry push subscriptions across a transport replacement.

        Reconnect previously created a bare transport, silently dropping
        user ``on_*`` handlers and the internal tick/book caches.
        """
        try:
            for cmd, callbacks in old._listeners.items():
                for cb in tuple(callbacks):
                    # Internal caches are re-registered explicitly below to
                    # avoid bound-method duplicates (which would double-cache).
                    func = getattr(cb, "__func__", None)
                    if func is not None and func.__name__ in {"_cache_tick_push", "_cache_book_push"}:
                        continue
                    new._listeners[cmd].add(cb)
        except Exception:
            logger.debug("listener migration failed", exc_info=True)
        try:
            for handler in tuple(old._callback_error_handlers):
                if handler not in new._callback_error_handlers:
                    new._callback_error_handlers.append(handler)
        except Exception:
            logger.debug("callback-error-handler migration failed", exc_info=True)
        new.on(CMD_TICK_PUSH, self._cache_tick_push)
        new.on(CMD_BOOK_PUSH, self._cache_book_push)

    async def _reconnect_loop(self) -> None:
        """Try to reconnect and re-login with stored credentials.

        Uses exponential backoff with jitter:
        ``min(base_delay * 2^(attempt-1) + random(0, base_delay), max_delay)``

        ``max_reconnect_attempts`` of 0 or None means retry forever with the
        same capped backoff. When a finite round is exhausted the transport
        stays in ERROR, but the next public call triggers a fresh round via
        :meth:`_recover_transport_on_demand` instead of failing forever.

        The attempt body runs under ``_conn_lock`` so an explicit
        ``connect()`` can never build a second transport beside it; a
        generation counter aborts attempts superseded while they slept.
        Per-attempt chatter stays at debug - one summary line per outcome.
        """
        max_attempts = self._max_reconnect_attempts
        infinite = max_attempts is None or max_attempts <= 0
        attempt = 0
        flap: dict[str, Any] = (
            self._flap if self._flap is not None else {"start": time.monotonic(), "attempts": 0, "last_error": None}
        )
        self._flap = flap
        try:
            while True:
                attempt += 1
                if max_attempts is not None and max_attempts > 0 and attempt > max_attempts:
                    break
                if self._closing:
                    logger.debug("reconnect aborted: client is closing")
                    return
                delay = min(
                    self._reconnect_delay * (2 ** (attempt - 1)) + random.uniform(0, self._reconnect_delay),
                    self._max_reconnect_delay,
                )
                flap["attempts"] += 1
                if infinite:
                    logger.debug("reconnect attempt %d (retrying until connected; delay=%.1fs)", attempt, delay)
                else:
                    logger.debug("reconnect attempt %d/%d (delay=%.1fs)", attempt, max_attempts, delay)
                if self._metrics:
                    try:
                        self._metrics.on_reconnect_attempt(attempt)
                    except Exception:
                        logger.debug("metrics on_reconnect_attempt raised", exc_info=True)
                await asyncio.sleep(delay)
                if self._closing:
                    logger.debug("reconnect aborted: client is closing")
                    return
                my_gen = self._transport_generation
                async with self._conn_lock:
                    if self._closing or self._transport_generation != my_gen:
                        # Superseded while sleeping: a newer round owns
                        # recovery now. Close nothing, emit nothing.
                        logger.debug("reconnect attempt %d superseded; aborting quietly", attempt)
                        return
                    if self.transport.is_ready and self._logged_in:
                        # Healed by other means (e.g. a manual login) while
                        # this round slept: stand down without touching it.
                        logger.debug("reconnect standing down: session already healthy")
                        self._flap = None
                        return
                    try:
                        built = False
                        if self.transport.is_ready:
                            # Explicit connect() won the race and finished the
                            # handshake; adopt it instead of tearing it down.
                            logger.debug("reconnect adopting ready transport; skipping rebuild")
                            adopted = self.transport
                            adopted._on_disconnect = self._handle_disconnect
                            adopted._on_demand_recover = self._recover_transport_on_demand
                        else:
                            # Close old transport to release resources
                            old_transport = self.transport
                            try:
                                await old_transport.close()
                            except Exception:
                                pass
                            # Reset transport for fresh connection
                            new_transport = MT5WebSocketTransport(
                                uri=self.uri,
                                timeout=self.timeout,
                                rate_limit=self._rate_limit,
                                rate_burst=self._rate_burst,
                                metrics=self._metrics,
                                ws_ping_interval=self._ws_ping_interval,
                                ws_ping_timeout=self._ws_ping_timeout,
                            )
                            self._migrate_transport_listeners(old_transport, new_transport)
                            self.transport = new_transport
                            self._transport_generation += 1
                            new_transport._on_disconnect = self._handle_disconnect
                            new_transport._on_demand_recover = self._recover_transport_on_demand
                            await new_transport.connect()
                            built = True
                        # Re-login with stored credentials
                        if self._login_kwargs is None:
                            raise SessionError("cannot reconnect: no stored credentials")
                        kwargs = dict(self._login_kwargs)
                        kwargs["auto_heartbeat"] = True
                        await self.login(**kwargs)
                        # Re-subscribe to ticks if we had subscriptions
                        if self._subscribed_ids:
                            try:
                                await self.subscribe_ticks(self._subscribed_ids)
                            except Exception:
                                logger.warning("tick resubscribe after reconnect failed", exc_info=True)
                        # Re-subscribe to order book if we had subscriptions
                        if self._subscribed_book_ids:
                            try:
                                await self.subscribe_book(self._subscribed_book_ids)
                            except Exception:
                                logger.warning("book resubscribe after reconnect failed", exc_info=True)
                        self._reconnect_count += 1
                        if built:
                            self._connect_count += 1
                        self._connected_at = time.monotonic()
                        self._heartbeat_failures = 0
                        self._last_heartbeat_ok = 0.0
                        episode = self._flap
                        if episode is not None:
                            logger.info(
                                "reconnected after %d attempt(s) in %.1fs",
                                episode["attempts"],
                                time.monotonic() - episode["start"],
                            )
                            self._flap = None
                        else:
                            logger.info("reconnected successfully on attempt %d", attempt)
                        if self._metrics:
                            try:
                                self._metrics.on_reconnect_success(attempt)
                            except Exception:
                                logger.debug("metrics on_reconnect_success raised", exc_info=True)
                        return
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.debug("reconnect attempt %d failed: %s", attempt, exc)
                        flap["last_error"] = str(exc)
            if max_attempts is not None and max_attempts > 0:
                episode = self._flap
                if episode is not None:
                    logger.warning(
                        "reconnect gave up after %d attempt(s) in %.1fs; last error: %s; next call will retry",
                        episode["attempts"],
                        time.monotonic() - episode["start"],
                        episode.get("last_error"),
                    )
                else:
                    logger.warning(
                        "all %d reconnect attempts exhausted; a later call will trigger a fresh round",
                        max_attempts,
                    )
        finally:
            if self._reconnect_task is asyncio.current_task():
                self._reconnect_task = None

    # ---- Health monitoring (Phase 16.3) ----

    async def health_check(self) -> HealthStatus:
        """Return a snapshot of the current connection health.

        If the transport is ready, a ping is sent and the round-trip
        latency is measured. Otherwise ``ping_latency_ms`` will be ``None``.
        """
        ping_latency_ms: float | None = None
        if self.transport.is_ready:
            t0 = time.monotonic()
            try:
                await self.ping()
                ping_latency_ms = (time.monotonic() - t0) * 1000.0
            except Exception:
                ping_latency_ms = None

        last_msg = self.transport._last_message_at if self.transport._last_message_at > 0 else None
        uptime = (time.monotonic() - self._connected_at) if self._connected_at > 0 else 0.0

        status = HealthStatus(
            state=self.transport.state,
            ping_latency_ms=ping_latency_ms,
            last_message_at=last_msg,
            uptime_seconds=uptime,
            reconnect_count=self._reconnect_count,
        )

        # Check degraded health thresholds
        if ping_latency_ms is not None and ping_latency_ms > self._health_degraded_threshold_ms:
            for cb in self._health_degraded_callbacks:
                try:
                    cb(status)
                except Exception as exc:
                    logger.error("health_degraded callback error: %s", exc)

        return status

    def on_health_degraded(
        self,
        callback: Callable[[HealthStatus], None],
        threshold_ms: float = 5000.0,
    ) -> None:
        """Register a callback that fires when ping latency exceeds *threshold_ms*.

        The callback receives the :class:`HealthStatus` snapshot.
        """
        self._health_degraded_threshold_ms = threshold_ms
        self._health_degraded_callbacks.append(callback)

    def connection_stats(self) -> ConnectionStats:
        """Return cumulative connection counters (local reads, never blocks)."""
        return ConnectionStats(
            connects=self._connect_count,
            disconnects=self._disconnect_count,
            reconnects=self._reconnect_count,
            heartbeat_failures=self._heartbeat_failure_total,
            last_error=self._last_connection_error,
            last_message_age=self.transport.last_message_age,
        )

    async def send_raw_command(self, command: int, payload: bytes | None = None) -> CommandResult:
        """Send a raw MT5 command.

        This is the escape hatch for reserved or reverse-engineered commands
        that do not have a first-class helper yet.
        """
        if command in {CMD_INIT, CMD_LOGIN, 52}:
            # These commands change the connection state in ways that make the
            # bootstrap-only reserved helper unsafe to reuse on the same socket.
            self._bootstrap_pristine = False
        return await self.transport.send_command(command, payload or b"")

    async def send_bootstrap_command_52(self) -> CommandResult:
        """Send the reserved bootstrap-only ``cmd=52`` helper.

        Observed against the official Web Terminal build 5687 (built on
        2026-03-15):

        - on a fresh bootstrap-only connection, ``cmd=52`` returns ``code=0``
          with an empty body
        - after ``cmd=29`` or ``cmd=28``, the same command causes the server to
          drop the socket

        The numeric ID is kept in the public name intentionally because the
        business meaning is still unknown.
        """
        if not self.transport.is_ready:
            raise SessionError("transport not ready")
        if self._logged_in or not self._bootstrap_pristine:
            raise SessionError(
                "cmd=52 is only safe on a fresh bootstrap-only connection; "
                "create a new client and call it before init_session() or login()",
            )
        self._bootstrap_pristine = False
        return await self.transport.send_command(52)

    async def init_session(
        self,
        version: int = 0,
        password: str = "",
        otp: str = "",
        cid: bytes | None = None,
    ) -> CommandResult:
        self._bootstrap_pristine = False
        payload = self._build_init_payload(
            version=version,
            password=password,
            otp=otp,
            cid=cid,
        )
        return await self.transport.send_command(CMD_INIT, payload)

    async def login(
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
    ) -> tuple[str, int]:
        self._bootstrap_pristine = False
        payload = self._build_login_payload(
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
        )
        result = await self.transport.send_command(CMD_LOGIN, payload)
        token_bytes, session_id = SeriesCodec.parse(result.body, LOGIN_RESPONSE_SCHEMA)
        self._logged_in = True
        # Store credentials for potential reconnect
        self._login_kwargs = {
            "login": login,
            "password": password,
            "url": url,
            "session": session,
            "otp": otp,
            "version": version,
            "cid": cid,
            "lead_cookie_id": lead_cookie_id,
            "lead_affiliate_site": lead_affiliate_site,
            "utm_campaign": utm_campaign,
            "utm_source": utm_source,
        }
        logger.info("logged in: login=%d session=%d", login, int(session_id))
        self._heartbeat_failures = 0
        self._last_heartbeat_ok = 0.0
        if auto_heartbeat:
            self._start_heartbeat()
        return bytes_to_hex(token_bytes), int(session_id)

    async def ping(self) -> None:
        await self.transport.send_command(CMD_PING)

    async def logout(self) -> None:
        self._stop_heartbeat()
        try:
            await self.transport.send_command(CMD_LOGOUT)
        finally:
            # Logout ends the session either way: a failed LOGOUT send
            # means the connection is already dead, so drop local session
            # state and credentials to avoid stale auto-reconnect.
            self._logged_in = False
            self._bootstrap_pristine = False
            self._clear_credentials()
        logger.info("logged out")

    def _clear_last_error(self) -> None:
        self._last_error = (0, "")

    def _fail_last_error(self, code: int, message: str) -> _T | None:
        self._last_error = (int(code), message)
        logger.debug("compat helper error %d: %s", code, message)
        return None

    def _resolve_client_id(self, cid: bytes | None) -> bytes:
        client_id = (
            bytes(cid)
            if cid is not None
            else build_client_id(
                platform=os.name,
                device_pixel_ratio="1",
                language="en-US",
                screen="0x0",
            )
        )
        if len(client_id) != 16:
            raise ValidationError(f"cid must be 16 bytes, got {len(client_id)}")
        return client_id

    def _build_login_payload(
        self,
        *,
        login: int,
        password: str,
        url: str,
        session: int,
        otp: str,
        version: int,
        cid: bytes | None,
        lead_cookie_id: int,
        lead_affiliate_site: str,
        utm_campaign: str,
        utm_source: str,
    ) -> bytes:
        client_id = self._resolve_client_id(cid)
        password_prefix, password_blob = self._split_password_blob(password)
        fields: list[tuple[Any, ...]] = [
            (PROP_U32, version or 0),
            (PROP_FIXED_STRING, password_prefix, 64),
            (PROP_FIXED_STRING, (otp or "")[:64], 128),
            (PROP_BYTES, client_id, 16),
            (PROP_FIXED_STRING, (utm_campaign or "")[:32], 64),
            (PROP_FIXED_STRING, (utm_source or "")[:32], 64),
            (PROP_U64, lead_cookie_id or 0),
            (PROP_FIXED_STRING, (lead_affiliate_site or "")[:64], 128),
            (PROP_U32, min(len(url or ""), 128)),
            (PROP_FIXED_STRING, (url or "")[:128], 256),
            (PROP_U64, int(login)),
            (PROP_BYTES, password_blob or bytes(160), 160),
            (PROP_U64, int(session or 0)),
        ]
        return SeriesCodec.serialize(fields)

    def _build_init_payload(
        self,
        *,
        version: int,
        password: str,
        otp: str,
        cid: bytes | None,
    ) -> bytes:
        client_id = self._resolve_client_id(cid)
        fields: list[tuple[Any, ...]] = [
            (PROP_U32, version or 0),
            (PROP_FIXED_STRING, (password or "")[:32], 64),
            (PROP_FIXED_STRING, (otp or "")[:64], 128),
            (PROP_BYTES, client_id, 16),
            (PROP_FIXED_STRING, "", 64),
            (PROP_FIXED_STRING, "", 64),
            (PROP_U64, 0),
            (PROP_FIXED_STRING, "", 128),
            (PROP_U32, 0),
            (PROP_FIXED_STRING, "", 256),
            (PROP_U64, 0),
        ]
        return SeriesCodec.serialize(fields)

    def _build_otp_setup_payload(
        self,
        *,
        login: int,
        password: str,
        otp: str = "",
        otp_secret: str = "",
        otp_secret_check: str = "",
        cid: bytes | None,
    ) -> bytes:
        client_id = self._resolve_client_id(cid)
        password_prefix, password_blob = self._split_password_blob(password)
        fields: list[tuple[Any, ...]] = [
            (PROP_U32, 5),
            (PROP_U64, int(login)),
            (PROP_FIXED_STRING, password_prefix, 64),
            (PROP_FIXED_STRING, (otp or "")[:64], 128),
            (PROP_FIXED_STRING, (otp_secret or "")[:64], 128),
            (PROP_FIXED_STRING, (otp_secret_check or "")[:64], 128),
            (PROP_BYTES, client_id, 16),
        ]
        if password_blob is not None:
            fields.append((PROP_BYTES, password_blob))
        return SeriesCodec.serialize(fields)
