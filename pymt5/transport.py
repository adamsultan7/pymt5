import asyncio
import contextlib
import enum
import inspect as _inspect
import struct
import time
import traceback
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import websockets
import websockets.asyncio.client as _ws_async_client
from websockets.asyncio.client import ClientConnection

from pymt5._logging import get_logger
from pymt5._metrics import MetricsCollector
from pymt5._rate_limiter import TokenBucketRateLimiter
from pymt5.constants import CMD_BOOTSTRAP, DEFAULT_COMMAND_TIMEOUT, DEFAULT_TOKEN_LENGTH, VALID_COMMANDS
from pymt5.crypto import AESCipher, initial_cipher
from pymt5.exceptions import MT5ConnectionError, MT5TimeoutError, ProtocolError, SessionError
from pymt5.protocol import ResponseFrame, build_command, pack_outer, parse_response_frame, unpack_outer

logger = get_logger("pymt5.transport")

# Use the asyncio WebSocket client (not the legacy websockets.connect).
# Kept as a module attribute so tests can patch
# ``pymt5.transport._ws_async_client.connect``.
_WS_CONNECT_HAS_PROXY = "proxy" in _inspect.signature(_ws_async_client.connect).parameters


class TransportState(enum.Enum):
    """Connection lifecycle states for the WebSocket transport."""

    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    READY = "ready"
    CLOSING = "closing"
    ERROR = "error"


@dataclass(slots=True)
class CommandResult:
    command: int
    code: int
    body: bytes


class MT5WebSocketTransport:
    def __init__(
        self,
        uri: str,
        timeout: float = DEFAULT_COMMAND_TIMEOUT,
        rate_limit: float = 0,
        rate_burst: int = 20,
        metrics: MetricsCollector | None = None,
        ws_ping_interval: float | None = None,
        ws_ping_timeout: float | None = None,
    ):
        self.uri = uri
        self.timeout = timeout
        self.ws: ClientConnection | None = None
        self._state = TransportState.DISCONNECTED
        self.token = bytes(DEFAULT_TOKEN_LENGTH)
        self.cipher: AESCipher = initial_cipher()
        self._recv_task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self._rate_limiter = TokenBucketRateLimiter(rate=rate_limit, burst=rate_burst)
        self._pending: dict[int, deque[asyncio.Future[CommandResult]]] = defaultdict(deque)
        self._listeners: dict[int, set[Callable[[CommandResult], Awaitable[None] | None]]] = defaultdict(set)
        self._on_disconnect: Callable[[], None] | None = None
        self._on_demand_recover: Callable[[], Awaitable[Any]] | None = None
        self._shutdown_event = asyncio.Event()
        self._disconnect_lock = asyncio.Lock()
        self._connect_lock = asyncio.Lock()
        self._metrics = metrics
        self._last_message_at: float = 0.0
        self._connected_at: float = 0.0
        self._callback_error_handlers: list[Callable] = []
        self._server_build: int = 0
        self._ws_ping_interval = ws_ping_interval
        self._ws_ping_timeout = ws_ping_timeout
        # Reference-client parity: the official Web Terminal runs in a
        # browser, which cannot initiate websocket pings, so it never
        # executes a session over a missed pong. pymt5 defaults to the same
        # passive posture (ping_interval=None sends nothing, enforces
        # nothing; server pings are still auto-answered at protocol level).
        # Liveness is owned 100% by the app-level heartbeat (CMD 51 ping +
        # staleness rule). Pass explicit ping_interval/ping_timeout values
        # to opt back into library-level kills.
        self.last_disconnect_reason: str | None = None

    @property
    def state(self) -> TransportState:
        """Current transport connection state."""
        return self._state

    @property
    def is_ready(self) -> bool:
        """Whether the transport is ready to send commands."""
        return self._state == TransportState.READY

    @is_ready.setter
    def is_ready(self, value: bool) -> None:
        """Backward-compatible setter for is_ready."""
        self._state = TransportState.READY if value else TransportState.DISCONNECTED

    @property
    def server_build(self) -> int:
        """Server build number extracted from the bootstrap response prefix."""
        return self._server_build

    @property
    def last_message_age(self) -> float | None:
        """Seconds since the last inbound message, or None if none received."""
        if self._last_message_at <= 0:
            return None
        return time.monotonic() - self._last_message_at

    def _is_ws_open(self) -> bool:
        """Best-effort check that the underlying socket is still open.

        Only returns False on affirmative evidence of closure (real
        ``State.CLOSED/CLOSING``, ``closed is True``, or an int close
        code). Mocks and unknown states are treated as open so unit
        tests with ``AsyncMock`` sockets keep working.
        """
        ws = self.ws
        if ws is None:
            return False
        try:
            state = getattr(ws, "state", None)
        except Exception:
            state = None
        if state is not None:
            name = getattr(state, "name", None)
            if isinstance(name, str):
                if name in ("CLOSED", "CLOSING"):
                    return False
                if name in ("OPEN", "CONNECTING"):
                    return True
                # Unknown string state: assume open.
                return True
            # Non-string .name (e.g. Mock): fall through to other checks.
        try:
            closed = getattr(ws, "closed", None)
        except Exception:
            closed = None
        if isinstance(closed, bool):
            return not closed
        try:
            close_code = getattr(ws, "close_code", None)
        except Exception:
            close_code = None
        return not isinstance(close_code, int)

    def _recv_task_alive(self) -> bool:
        task = self._recv_task
        return task is not None and not task.done()

    async def connect(self) -> None:
        async with self._connect_lock:
            await self._connect_locked()

    async def _connect_locked(self) -> None:
        # Guard against double-connect (Phase 2.4)
        if self.ws is not None:
            await self.close()
        self._state = TransportState.CONNECTING
        self._shutdown_event.clear()
        self.cipher = initial_cipher()
        logger.debug("connecting to %s", self.uri)
        connect_kwargs: dict[str, Any] = {
            "ping_interval": self._ws_ping_interval,
            "ping_timeout": self._ws_ping_timeout,
            "max_size": None,
            "open_timeout": self.timeout,
            "additional_headers": {
                "Origin": "https://web.metatrader.app",
            },
        }
        # websockets auto-detects system proxy (proxy=True default)
        # which breaks the MT5 binary protocol; bypass it explicitly.
        if _WS_CONNECT_HAS_PROXY:
            connect_kwargs["proxy"] = None
        try:
            self.ws = await asyncio.wait_for(
                _ws_async_client.connect(self.uri, **connect_kwargs),
                timeout=self.timeout,
            )
        except Exception:
            self._state = TransportState.ERROR
            self.ws = None
            raise
        self._recv_task = asyncio.create_task(self._recv_loop())
        logger.debug("websocket open, sending bootstrap")
        try:
            bootstrap = await self._send_raw(CMD_BOOTSTRAP, self.token, check_ready=False)
        except Exception:
            await self._cleanup_failed_connect()
            raise
        if bootstrap.code != 0:
            await self._cleanup_failed_connect()
            raise MT5ConnectionError(f"bootstrap failed: code={bootstrap.code}")
        if len(bootstrap.body) < 66:
            await self._cleanup_failed_connect()
            raise MT5ConnectionError(f"bootstrap response too short: {len(bootstrap.body)}")
        self.token = bootstrap.body[2:66]
        self.cipher = AESCipher(bootstrap.body[66:])
        # Try to extract server build from the 2-byte prefix (U16 LE)
        try:
            self._server_build = struct.unpack_from("<H", bootstrap.body, 0)[0]
        except struct.error:
            self._server_build = 0
        self._state = TransportState.READY
        self._connected_at = time.monotonic()
        if self._metrics:
            try:
                self._metrics.on_connect()
            except Exception:
                logger.debug("metrics on_connect raised", exc_info=True)
        logger.debug("transport ready (key exchanged)")

    async def _cleanup_failed_connect(self) -> None:
        """Release socket/recv-task after a failed handshake; state -> ERROR."""
        if self._recv_task is not None:
            self._recv_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._recv_task
            self._recv_task = None
        if self.ws is not None:
            with contextlib.suppress(Exception):
                await self.ws.close()
            self.ws = None
        self._fail_all(MT5ConnectionError("connect handshake failed"))
        self._state = TransportState.ERROR

    async def close(self) -> None:
        logger.debug("closing transport")
        async with self._disconnect_lock:
            self._state = TransportState.CLOSING
            self._shutdown_event.set()
        if self._recv_task is not None:
            self._recv_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._recv_task
            self._recv_task = None
        if self.ws is not None:
            with contextlib.suppress(Exception):
                await self.ws.close()
            self.ws = None
        self._fail_all(SessionError("transport closed"))
        self._state = TransportState.DISCONNECTED
        logger.debug("transport closed")

    def on(self, command: int, callback: Callable[[CommandResult], Awaitable[None] | None]) -> None:
        self._listeners[command].add(callback)

    def off(self, command: int, callback: Callable[[CommandResult], Awaitable[None] | None] | None = None) -> None:
        if callback is None:
            self._listeners[command].clear()
            return
        self._listeners[command].discard(callback)

    async def send_command(self, command: int, payload: bytes | None = None) -> CommandResult:
        return await self._send_raw(command, payload or b"", check_ready=True)

    async def _handle_connection_loss(self, exc: BaseException) -> None:
        """Mark the transport as broken, fail pending calls, notify once.

        Safe to call from the recv loop, send path, or health checks.
        No-op when the transport is shutting down explicitly.
        """
        if self._shutdown_event.is_set() or self._state == TransportState.CLOSING:
            return
        already_error = self._state == TransportState.ERROR
        self._state = TransportState.ERROR
        self.last_disconnect_reason = str(exc) or type(exc).__name__
        if isinstance(exc, (OSError, websockets.exceptions.WebSocketException)):
            fail_exc: Exception = MT5ConnectionError(str(exc) or "websocket connection lost")
            fail_exc.__cause__ = exc
        elif isinstance(exc, Exception):
            fail_exc = exc
        else:
            fail_exc = MT5ConnectionError(f"websocket connection lost: {exc!r}")
        self._fail_all(fail_exc)
        if self._metrics and not already_error:
            try:
                self._metrics.on_disconnect(str(exc))
            except Exception:
                pass
        if already_error:
            return
        logger.error("transport disconnected: %s", exc)
        should_notify = False
        try:
            async with self._disconnect_lock:
                if self._on_disconnect and not self._shutdown_event.is_set():
                    should_notify = True
        except asyncio.CancelledError:
            raise
        except Exception:
            should_notify = False
        if should_notify and self._on_disconnect:
            try:
                self._on_disconnect()
            except Exception:
                logger.warning("on_disconnect handler raised", exc_info=True)

    def _ensure_usable(self, command: int, check_ready: bool) -> None:
        if check_ready and self._state != TransportState.READY:
            raise SessionError(f"transport not ready for command {command} (state={self._state.value})")
        if self.ws is None:
            raise MT5ConnectionError("websocket not connected")
        if check_ready and not self._is_ws_open():
            # Socket already closed at the websockets level but our state
            # has not flipped yet (half-open / clean close race).
            raise MT5ConnectionError("websocket connection is closed")
        if check_ready and not self._recv_task_alive():
            recv = self._recv_task
            if recv is not None and recv.done():
                recv_exc: BaseException | None = None
                try:
                    recv_exc = recv.exception()
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                detail = f": {recv_exc}" if recv_exc else ""
                raise MT5ConnectionError(f"connection receiver is not running{detail}")

    def _drop_pending(self, command: int, future: asyncio.Future[CommandResult]) -> None:
        if future.done():
            return
        queue = self._pending.get(command)
        if queue:
            try:
                queue.remove(future)
            except ValueError:
                pass

    async def _send_raw(
        self, command: int, payload: bytes, check_ready: bool, _allow_lazy_recovery: bool = True
    ) -> CommandResult:
        if command not in VALID_COMMANDS:
            raise ProtocolError(f"unsupported command: {command}")
        try:
            self._ensure_usable(command, check_ready)
        except SessionError:
            if (
                not check_ready
                or not _allow_lazy_recovery
                or self._state != TransportState.ERROR
                or self._on_demand_recover is None
                or self._shutdown_event.is_set()
            ):
                raise
            live = await self._on_demand_recover()
            if live is None or not isinstance(live, MT5WebSocketTransport):
                raise
            if live is self:
                self._ensure_usable(command, check_ready)
            else:
                # Reconnect replaces the transport object: re-dispatch on the
                # live one without allowing a second recovery round (fail
                # closed instead of recursing on a flapping connection).
                return await live._send_raw(command, payload, check_ready, _allow_lazy_recovery=False)
        # self.ws is guaranteed non-None by _ensure_usable
        assert self.ws is not None
        await self._rate_limiter.acquire()
        # Re-check after rate-limit wait: the socket may have died while queued.
        try:
            self._ensure_usable(command, check_ready)
        except (MT5ConnectionError, SessionError) as exc:
            await self._handle_connection_loss(exc)
            raise
        async with self._lock:
            future: asyncio.Future[CommandResult] = asyncio.get_running_loop().create_future()
            self._pending[command].append(future)
            inner = build_command(command, payload)
            encrypted = self.cipher.encrypt(inner)
            logger.debug("send cmd=%d payload=%d bytes", command, len(payload))
            try:
                await self.ws.send(pack_outer(encrypted))
            except (OSError, websockets.exceptions.WebSocketException) as exc:
                self._drop_pending(command, future)
                if not future.done():
                    future.cancel()
                await self._handle_connection_loss(exc)
                raise MT5ConnectionError(f"send failed for command {command}: {exc}") from exc
            except Exception:
                self._drop_pending(command, future)
                if not future.done():
                    future.cancel()
                raise
            if self._metrics:
                try:
                    self._metrics.on_command_sent(command)
                except Exception:
                    logger.debug("metrics on_command_sent raised", exc_info=True)
        try:
            return await asyncio.wait_for(future, timeout=self.timeout)
        except TimeoutError:
            # Remove leaked future from _pending on timeout (Phase 2.1)
            self._drop_pending(command, future)
            if not future.done():
                future.cancel()
            # A lone timeout does not prove a dead socket, but if the
            # underlying ws is already closed the state must flip now so
            # the next call errors instead of hanging silently.
            if not self._is_ws_open() or not self._recv_task_alive():
                await self._handle_connection_loss(
                    MT5ConnectionError(f"command {command} timed out and connection looks dead")
                )
            raise MT5TimeoutError(f"command {command} timed out after {self.timeout}s") from None
        except (MT5ConnectionError, SessionError):
            raise
        except (OSError, websockets.exceptions.WebSocketException) as exc:
            # Future was failed by _handle_connection_loss with the raw error.
            raise MT5ConnectionError(f"command {command} failed: {exc}") from exc

    async def _recv_loop(self) -> None:
        try:
            ws = self.ws
            if ws is None:
                return
            async for message in ws:
                if isinstance(message, str):
                    continue
                try:
                    raw = message if isinstance(message, bytes) else bytes(message)
                    _, _, encrypted = unpack_outer(raw)
                    decrypted = self.cipher.decrypt(encrypted)
                    frame = parse_response_frame(decrypted)
                    self._last_message_at = time.monotonic()
                    logger.debug("recv cmd=%d code=%d body=%d bytes", frame.command, frame.code, len(frame.body))
                    await self._dispatch(frame)
                except (struct.error, ValueError, TypeError, IndexError, ProtocolError) as exc:
                    logger.error("recv_loop parse error: %s", exc)
                    continue
            # Normal loop exit == server cleanly closed the socket.
            # Previously this silently left state==READY forever.
            if not self._shutdown_event.is_set():
                await self._handle_connection_loss(MT5ConnectionError("server closed the connection"))
        except asyncio.CancelledError:
            raise
        except (OSError, websockets.exceptions.WebSocketException) as exc:
            await self._handle_connection_loss(exc)
        except Exception as exc:
            await self._handle_connection_loss(exc)

    async def _dispatch(self, frame: ResponseFrame) -> None:
        result = CommandResult(command=frame.command, code=frame.code, body=frame.body)
        if self._metrics:
            try:
                self._metrics.on_command_received(frame.command, frame.code)
            except Exception:
                logger.debug("metrics on_command_received raised", exc_info=True)
        queue = self._pending.get(frame.command)
        if queue:
            while queue:
                future = queue.popleft()
                if not future.done():
                    future.set_result(result)
                    break
        for callback in tuple(self._listeners.get(frame.command, ())):
            try:
                maybe = callback(result)
                if _inspect.isawaitable(maybe):
                    await maybe
            except Exception as exc:
                logger.error(
                    "callback %s raised %s:\n%s",
                    getattr(callback, "__name__", repr(callback)),
                    exc,
                    traceback.format_exc(),
                )
                for error_handler in tuple(self._callback_error_handlers):
                    try:
                        error_handler(exc, callback)
                    except Exception:
                        logger.warning(
                            "callback error handler %s itself raised",
                            getattr(error_handler, "__name__", repr(error_handler)),
                            exc_info=True,
                        )

    def _fail_all(self, exc: Exception) -> None:
        for queue in self._pending.values():
            while queue:
                future = queue.popleft()
                if not future.done():
                    future.set_exception(exc)
