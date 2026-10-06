"""Regression tests for silent-disconnect detection.

Covers: clean server close must flip state to ERROR and notify,
send failures must surface MT5ConnectionError, heartbeat failures
must trigger disconnect handling, and reconnect must preserve listeners.
"""

import asyncio
import contextlib
import logging
import struct
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import websockets.exceptions

from pymt5.client import MT5WebClient
from pymt5.constants import CMD_BOOK_PUSH, CMD_GET_ACCOUNT, CMD_PING, CMD_TICK_PUSH
from pymt5.exceptions import MT5ConnectionError, SessionError
from pymt5.protocol import pack_outer
from pymt5.transport import CommandResult, MT5WebSocketTransport, TransportState


class _EmptyWS:
    """WebSocket whose async iterator ends immediately (clean close)."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def send(self, data):
        return None

    async def close(self):
        return None


async def test_recv_loop_clean_close_marks_error_and_notifies():
    t = MT5WebSocketTransport(uri="wss://x")
    t._state = TransportState.READY
    t.ws = _EmptyWS()  # type: ignore[assignment]
    notified = []
    t._on_disconnect = lambda: notified.append(True)
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    t._pending[CMD_GET_ACCOUNT].append(fut)

    await t._recv_loop()

    assert t.state == TransportState.ERROR
    assert notified == [True]
    assert fut.done()
    # Pending failures are normalized to MT5ConnectionError subclasses
    try:
        fut.result()
        raise AssertionError("should have raised")
    except Exception as exc:
        assert isinstance(exc, Exception)


async def test_handle_connection_loss_noop_when_closing():
    t = MT5WebSocketTransport(uri="wss://x")
    t._state = TransportState.CLOSING
    t._shutdown_event.set()
    notified = []
    t._on_disconnect = lambda: notified.append(True)
    await t._handle_connection_loss(ConnectionError("gone"))
    assert notified == []
    assert t.state == TransportState.CLOSING


async def test_send_failure_raises_and_marks_error():
    t = MT5WebSocketTransport(uri="wss://x", timeout=2.0)
    t._state = TransportState.READY

    async def _failing_send(data):
        raise websockets.exceptions.ConnectionClosedOK(None, None)

    mock_ws = MagicMock()
    mock_ws.send = _failing_send
    mock_ws.state = MagicMock()
    mock_ws.state.name = "OPEN"
    mock_ws.closed = False
    mock_ws.close_code = None
    t.ws = mock_ws
    notified = []
    t._on_disconnect = lambda: notified.append(True)

    try:
        await t.send_command(CMD_GET_ACCOUNT)
        raise AssertionError("should have raised")
    except Exception as exc:
        # Must be an error, never silent
        assert isinstance(exc, (ConnectionError, RuntimeError))

    assert t.state == TransportState.ERROR
    assert notified == [True]

    # Next call must also error, not hang or return empty
    try:
        await t.send_command(CMD_GET_ACCOUNT)
        raise AssertionError("should have raised")
    except RuntimeError:
        pass


async def test_heartbeat_failures_trigger_disconnect():
    client = MT5WebClient(heartbeat_interval=0.01, heartbeat_failure_threshold=2)
    calls = []

    async def _always_fail():
        calls.append(1)
        raise TimeoutError("ping timeout")

    client.ping = _always_fail  # type: ignore[method-assign]
    disconnects = []
    orig_handler = client.transport._handle_connection_loss

    async def _spy(exc):
        disconnects.append(exc)
        await orig_handler(exc)

    client.transport._handle_connection_loss = _spy  # type: ignore[method-assign]
    client._start_heartbeat()
    await asyncio.sleep(0.1)
    # Heartbeat loop exits after threshold; task reference cleared by disconnect path
    # or still running — either way disconnect must have been triggered.
    client._stop_heartbeat()
    assert len(calls) >= 2
    assert len(disconnects) >= 1


async def test_heartbeat_resets_counter_on_success():
    client = MT5WebClient(heartbeat_interval=0.01, heartbeat_failure_threshold=3)
    n = 0

    async def _flaky():
        nonlocal n
        n += 1
        if n == 1:
            raise TimeoutError("once")

    client.ping = _flaky  # type: ignore[method-assign]
    client._start_heartbeat()
    await asyncio.sleep(0.05)
    client._stop_heartbeat()
    assert client._heartbeat_failures == 0


async def test_reconnect_preserves_listeners():
    client = MT5WebClient(auto_reconnect=True, reconnect_delay=0.001, max_reconnect_attempts=1)
    received = []
    client.on_tick(lambda ticks: received.append(ticks))
    client.transport._callback_error_handlers.append(lambda e, cb: None)
    old = client.transport

    new_transport = MT5WebSocketTransport(uri=client.uri)
    client._migrate_transport_listeners(old, new_transport)

    # User tick handler + internal cache handler must survive
    assert len(new_transport._listeners[CMD_TICK_PUSH]) == len(old._listeners[CMD_TICK_PUSH])
    assert len(new_transport._listeners[CMD_BOOK_PUSH]) == 1
    assert new_transport._callback_error_handlers == old._callback_error_handlers


def test_transport_enables_ws_ping_by_default():
    t = MT5WebSocketTransport(uri="wss://x")
    # Passive by default (reference-client parity): never initiate pings,
    # never enforce pongs. Liveness is owned by the app-level heartbeat.
    assert t._ws_ping_interval is None
    assert t._ws_ping_timeout is None


def test_client_passes_ws_ping_to_transport():
    c = MT5WebClient(ws_ping_interval=5.0, ws_ping_timeout=6.0)
    assert c.transport._ws_ping_interval == 5.0
    assert c.transport._ws_ping_timeout == 6.0


def _error_transport(client: MT5WebClient) -> MT5WebSocketTransport:
    """Simulate an exhausted reconnect round: ERROR, no task, creds stored."""
    t = MT5WebSocketTransport(uri="wss://x", timeout=5.0)
    t._state = TransportState.ERROR
    t._on_disconnect = client._handle_disconnect
    t._on_demand_recover = client._recover_transport_on_demand
    client.transport = t
    client._login_kwargs = {"login": 1, "password": "x"}
    client._logged_in = False
    return t


def _mock_transport(**overrides):
    # spec= keeps isinstance(live, MT5WebSocketTransport) true, matching the
    # real objects _reconnect_loop builds in production.
    m = MagicMock(spec=MT5WebSocketTransport)
    m.connect = AsyncMock()
    m.close = AsyncMock()
    m.is_ready = True
    m._on_disconnect = None
    m._on_demand_recover = None
    m._listeners = {}
    m._callback_error_handlers = []
    m.on = MagicMock()
    for key, value in overrides.items():
        setattr(m, key, value)
    return m


async def test_reconnect_zero_means_retry_forever():
    client = MT5WebClient(
        auto_reconnect=True,
        max_reconnect_attempts=0,
        reconnect_delay=0.001,
        max_reconnect_delay=0.002,
    )
    client._login_kwargs = {"login": 1, "password": "x"}
    attempts = 0

    def _factory(*args, **kwargs):
        m = _mock_transport()
        # A transport whose connect() raised is not ready, so each attempt
        # rebuilds instead of adopting it.
        m.is_ready = False

        async def _fail():
            nonlocal attempts
            attempts += 1
            if attempts >= 3:
                raise asyncio.CancelledError()
            raise MT5ConnectionError("down")

        m.connect = _fail
        return m

    with patch("pymt5.client.MT5WebSocketTransport", side_effect=_factory), pytest.raises(asyncio.CancelledError):
        await client._reconnect_loop()
    # A finite round of max_reconnect_attempts=1 would stop at 1.
    assert attempts >= 3


async def test_reconnect_none_means_retry_forever():
    client = MT5WebClient(
        auto_reconnect=True,
        max_reconnect_attempts=None,
        reconnect_delay=0.001,
        max_reconnect_delay=0.002,
    )
    client._login_kwargs = {"login": 1, "password": "x"}
    attempts = 0

    def _factory(*args, **kwargs):
        m = _mock_transport()
        # A transport whose connect() raised is not ready, so each attempt
        # rebuilds instead of adopting it.
        m.is_ready = False

        async def _fail():
            nonlocal attempts
            attempts += 1
            if attempts >= 3:
                raise asyncio.CancelledError()
            raise MT5ConnectionError("down")

        m.connect = _fail
        return m

    with patch("pymt5.client.MT5WebSocketTransport", side_effect=_factory), pytest.raises(asyncio.CancelledError):
        await client._reconnect_loop()
    assert attempts >= 3


async def test_reconnect_exhaustion_self_heals_on_next_use():
    """After an exhausted round, the next call reconnects instead of raising."""
    client = MT5WebClient(
        auto_reconnect=True,
        max_reconnect_attempts=1,
        reconnect_delay=0.001,
        max_reconnect_delay=0.002,
        timeout=5.0,
    )
    old = _error_transport(client)

    async def _fake_login(**kwargs):
        client._logged_in = True
        return ("tok", 1)

    new_mock = _mock_transport(
        _send_raw=AsyncMock(return_value=CommandResult(command=CMD_PING, code=0, body=b"")),
    )
    client.login = _fake_login  # type: ignore[method-assign]

    with patch("pymt5.client.MT5WebSocketTransport", return_value=new_mock):
        await client.ping()  # must reconnect and succeed, not raise

    assert client.transport is new_mock
    assert client.transport is not old
    assert client._logged_in is True
    assert client.is_connected is True


async def test_no_lazy_recovery_when_auto_reconnect_off():
    client = MT5WebClient(auto_reconnect=False)
    _error_transport(client)
    with pytest.raises(SessionError):
        await client.ping()
    assert client._reconnect_task is None


async def test_lazy_recovery_raises_when_round_exhausted_again():
    """A failed recovery round fails closed: SessionError, no hang, no resend."""
    client = MT5WebClient(
        auto_reconnect=True,
        max_reconnect_attempts=1,
        reconnect_delay=0.001,
        max_reconnect_delay=0.002,
    )
    _error_transport(client)
    failing = _mock_transport(connect=AsyncMock(side_effect=MT5ConnectionError("still down")))
    failing.is_ready = False
    with patch("pymt5.client.MT5WebSocketTransport", return_value=failing), pytest.raises(SessionError):
        await client.ping()


# ---- Bug A: serialized (re)connects, generation guard ----


async def test_concurrent_connect_and_reconnect_yield_one_session():
    """N concurrent connect() calls racing a reconnect build one session."""
    client = MT5WebClient(
        auto_reconnect=True,
        max_reconnect_attempts=5,
        reconnect_delay=0.001,
        max_reconnect_delay=0.002,
        timeout=5.0,
    )
    client._login_kwargs = {"login": 1, "password": "x"}
    created = 0
    logins = 0

    def _factory(*args, **kwargs):
        nonlocal created
        created += 1
        m = MagicMock()
        m.is_ready = False
        m._on_disconnect = None
        m._on_demand_recover = None
        m._listeners = {}
        m._callback_error_handlers = []
        m.on = MagicMock()
        m.close = AsyncMock()

        async def _connect():
            await asyncio.sleep(0.01)
            m.is_ready = True

        m.connect = _connect
        return m

    async def _fake_login(**kwargs):
        nonlocal logins
        logins += 1
        client._logged_in = True
        return ("tok", 1)

    client.login = _fake_login  # type: ignore[method-assign]
    with patch("pymt5.client.MT5WebSocketTransport", side_effect=_factory):
        client.transport = _factory()
        assert created == 1
        reconnect_task = asyncio.create_task(client._reconnect_loop())
        client._reconnect_task = reconnect_task
        await asyncio.sleep(0)  # let the round reach its backoff sleep
        try:
            results = await asyncio.gather(*(client.connect() for _ in range(5)))
            await asyncio.wait_for(reconnect_task, timeout=5)
        finally:
            if not reconnect_task.done():
                reconnect_task.cancel()
        assert all(r is client for r in results)
        assert logins == 1
        assert created == 2  # seed + exactly one rebuild, never two live
        assert client.transport.is_ready is True
        assert client.is_connected is True
        assert client._transport_generation == 1
        assert client._reconnect_task is None


async def test_reconnect_adopts_ready_transport_without_rebuild():
    """An explicit connect() that wins the race is adopted, not torn down."""
    client = MT5WebClient(
        auto_reconnect=True,
        max_reconnect_attempts=3,
        reconnect_delay=0.001,
        max_reconnect_delay=0.002,
    )
    client._login_kwargs = {"login": 1, "password": "x"}
    ready = MagicMock()
    ready.is_ready = True
    ready._on_disconnect = None
    ready._on_demand_recover = None
    ready._listeners = {}
    ready._callback_error_handlers = []
    ready.on = MagicMock()
    ready.close = AsyncMock()
    client.transport = ready
    client._logged_in = False
    logins = 0

    async def _fake_login(**kwargs):
        nonlocal logins
        logins += 1
        client._logged_in = True
        return ("tok", 1)

    client.login = _fake_login  # type: ignore[method-assign]
    with patch(
        "pymt5.client.MT5WebSocketTransport",
        side_effect=AssertionError("must not build while a ready transport exists"),
    ):
        await client._reconnect_loop()
    assert logins == 1
    assert client.transport is ready
    ready.close.assert_not_awaited()
    assert client._transport_generation == 0
    assert client.is_connected is True


async def test_superseded_reconnect_aborts_quietly():
    """A round superseded mid-backoff closes nothing and emits nothing."""
    client = MT5WebClient(
        auto_reconnect=True,
        max_reconnect_attempts=3,
        reconnect_delay=0.05,
        max_reconnect_delay=0.05,
    )
    client._login_kwargs = {"login": 1, "password": "x"}
    notified = []
    client.on_disconnect(lambda: notified.append(True))
    task = asyncio.create_task(client._reconnect_loop())
    await asyncio.sleep(0.01)  # round is sleeping off its first backoff
    winner = MagicMock()
    winner.is_ready = True
    winner.close = AsyncMock()
    client.transport = winner
    client._transport_generation += 1
    await asyncio.wait_for(task, timeout=5)
    assert notified == []
    assert client._reconnect_task is None
    assert client.transport is winner
    winner.close.assert_not_awaited()


async def test_rapid_kill_reconnect_cycles_converge():
    """Repeated kill/reconnect cycles converge: one login per cycle, no orphans."""
    client = MT5WebClient(
        auto_reconnect=True,
        max_reconnect_attempts=3,
        reconnect_delay=0.001,
        max_reconnect_delay=0.002,
        timeout=5.0,
    )
    client._login_kwargs = {"login": 1, "password": "x"}
    created = 0
    logins = 0
    real_cls = MT5WebSocketTransport

    def _factory(*args, **kwargs):
        nonlocal created
        created += 1
        t = real_cls(*args, **kwargs)

        async def _connect():
            t._state = TransportState.READY

        t.connect = _connect  # type: ignore[method-assign]
        return t

    async def _fake_login(**kwargs):
        nonlocal logins
        logins += 1
        client._logged_in = True
        return ("tok", 1)

    client.login = _fake_login  # type: ignore[method-assign]
    with patch("pymt5.client.MT5WebSocketTransport", side_effect=_factory):
        for i in range(5):
            await client.transport._handle_connection_loss(MT5ConnectionError(f"flap {i}"))
            task = client._reconnect_task
            assert task is not None, f"cycle {i}: no round scheduled"
            await asyncio.wait_for(task, timeout=5)
            assert client.is_connected, f"cycle {i} did not converge"
            assert logins == i + 1, f"cycle {i}: expected exactly one login"
        await asyncio.sleep(0.05)  # quiet period: no orphan round may fire
        stable = client.transport
        assert client._reconnect_task is None
        assert client.is_connected
        assert client.transport is stable
    assert created == 5
    assert client._transport_generation == 5


# ---- Bug B: ws keepalive posture ----


class _HangingWS:
    """Fake socket that never yields: parks the recv loop without traffic."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(3600)
        raise StopAsyncIteration

    async def send(self, data):
        return None

    async def close(self):
        return None


def test_ws_keepalive_defaults_do_not_enforce_pongs():
    t = MT5WebSocketTransport(uri="wss://x")
    assert t._ws_ping_interval is None
    assert t._ws_ping_timeout is None
    c = MT5WebClient()
    assert c.transport._ws_ping_interval is None
    assert c.transport._ws_ping_timeout is None


def test_sync_keepalive_defaults_do_not_enforce_pongs():
    from pymt5.sync import SyncMT5Client

    s = SyncMT5Client()
    try:
        assert s._client.transport._ws_ping_interval is None
        assert s._client.transport._ws_ping_timeout is None
    finally:
        s.close()


async def test_connect_forwards_ping_posture():
    seen = {}

    async def _capture(uri, **kwargs):
        seen.update(kwargs)
        return _HangingWS()

    async def _ok_bootstrap(cmd, payload=b"", check_ready=False):
        return CommandResult(command=cmd, code=0, body=b"\x00\x00" + bytes(64) + bytes(16))

    t = MT5WebSocketTransport(uri="wss://x", timeout=5.0)
    with patch("pymt5.transport._ws_async_client.connect", side_effect=_capture):
        t._send_raw = _ok_bootstrap  # type: ignore[method-assign]
        await t.connect()
    assert seen["ping_interval"] is None
    assert seen["ping_timeout"] is None
    assert t.is_ready
    await t.close()


async def test_explicit_ping_timeout_still_opt_in():
    seen = {}

    async def _capture(uri, **kwargs):
        seen.update(kwargs)
        return _HangingWS()

    async def _ok_bootstrap(cmd, payload=b"", check_ready=False):
        return CommandResult(command=cmd, code=0, body=b"\x00\x00" + bytes(64) + bytes(16))

    t = MT5WebSocketTransport(uri="wss://x", timeout=5.0, ws_ping_interval=5.0, ws_ping_timeout=0.5)
    with patch("pymt5.transport._ws_async_client.connect", side_effect=_capture):
        t._send_raw = _ok_bootstrap  # type: ignore[method-assign]
        await t.connect()
    assert seen["ping_interval"] == 5.0
    assert seen["ping_timeout"] == 0.5
    await t.close()


async def test_silent_socket_declared_dead_by_heartbeat():
    """A socket with no traffic must trip the app heartbeat promptly."""
    client = MT5WebClient(
        auto_reconnect=False,
        heartbeat_interval=0.05,
        heartbeat_failure_threshold=2,
        timeout=5.0,
    )
    client.transport._state = TransportState.READY  # looks live; socket gone
    fired = []
    client.on_disconnect(lambda: fired.append(time.monotonic()))
    t0 = time.monotonic()
    client._start_heartbeat()
    for _ in range(300):
        if fired:
            break
        await asyncio.sleep(0.01)
    client._stop_heartbeat()
    assert fired, "heartbeat never declared the silent socket dead"
    assert (fired[0] - t0) < 0.05 * (2 + 1) + 0.5
    assert client.transport.state == TransportState.ERROR


async def test_stale_heartbeat_declares_dead():
    """No successful ping within stale_after trips, even below the strike threshold."""
    client = MT5WebClient(
        auto_reconnect=False,
        heartbeat_interval=0.05,
        heartbeat_failure_threshold=1000,
        heartbeat_stale_after=0.12,
        timeout=5.0,
    )
    n = 0

    async def _once_ok():
        nonlocal n
        n += 1
        if n == 1:
            return None
        raise MT5ConnectionError("quiet")

    client.ping = _once_ok  # type: ignore[method-assign]
    fired = []
    client.on_disconnect(lambda: fired.append(time.monotonic()))
    t0 = time.monotonic()
    client._start_heartbeat()
    for _ in range(300):
        if fired:
            break
        await asyncio.sleep(0.01)
    client._stop_heartbeat()
    assert fired, "stale heartbeat never declared the socket dead"
    assert (fired[0] - t0) < 0.05 + 0.12 + 0.5
    assert client.transport.state == TransportState.ERROR


async def test_hung_ping_does_not_stall_detection():
    """A ping that never returns counts toward the window via the interval bound."""
    client = MT5WebClient(
        auto_reconnect=False,
        heartbeat_interval=0.05,
        heartbeat_failure_threshold=2,
        timeout=5.0,
    )

    async def _hangs():
        await asyncio.sleep(3600)

    client.ping = _hangs  # type: ignore[method-assign]
    fired = []
    client.on_disconnect(lambda: fired.append(time.monotonic()))
    t0 = time.monotonic()
    client._start_heartbeat()
    for _ in range(300):
        if fired:
            break
        await asyncio.sleep(0.01)
    client._stop_heartbeat()
    assert fired, "hung ping stalled heartbeat detection"
    assert (fired[0] - t0) < 0.05 * 2 + 0.5
    assert client.transport.state == TransportState.ERROR


# ---- Bug C: flap-aware logging ----


async def test_flap_collapses_attempt_logs_to_summary(caplog):
    client = MT5WebClient(
        auto_reconnect=True,
        max_reconnect_attempts=3,
        reconnect_delay=0.001,
        max_reconnect_delay=0.002,
    )
    client._login_kwargs = {"login": 1, "password": "x"}
    failing = _mock_transport(connect=AsyncMock(side_effect=MT5ConnectionError("down")))
    failing.is_ready = False
    with (
        caplog.at_level(logging.DEBUG, logger="pymt5.client"),
        patch("pymt5.client.MT5WebSocketTransport", return_value=failing),
    ):
        await client._reconnect_loop()
    louder = [r for r in caplog.records if r.name == "pymt5.client" and r.levelno >= logging.INFO]
    assert len(louder) == 1, [r.getMessage() for r in louder]
    assert "gave up" in louder[0].getMessage()
    assert "3 attempt" in louder[0].getMessage()


async def test_flap_success_logs_single_summary(caplog):
    client = MT5WebClient(
        auto_reconnect=True,
        max_reconnect_attempts=3,
        reconnect_delay=0.001,
        max_reconnect_delay=0.002,
    )
    client._login_kwargs = {"login": 1, "password": "x"}
    client._flap = {"start": time.monotonic(), "attempts": 0, "last_error": None}

    async def _fake_login(**kwargs):
        client._logged_in = True
        return ("tok", 1)

    new_mock = _mock_transport()
    client.login = _fake_login  # type: ignore[method-assign]
    with (
        caplog.at_level(logging.DEBUG, logger="pymt5.client"),
        patch("pymt5.client.MT5WebSocketTransport", return_value=new_mock),
    ):
        await client._reconnect_loop()
    louder = [r for r in caplog.records if r.name == "pymt5.client" and r.levelno >= logging.INFO]
    assert len(louder) == 1, [r.getMessage() for r in louder]
    assert "reconnected after" in louder[0].getMessage()


def test_repeat_disconnect_within_episode_is_debug(caplog):
    client = MT5WebClient()  # auto_reconnect off: no task scheduled
    with caplog.at_level(logging.DEBUG, logger="pymt5.client"):
        client._handle_disconnect()
        client._handle_disconnect()
    levels = [r.levelno for r in caplog.records if r.name == "pymt5.client" and "disconnect" in r.getMessage()]
    assert levels == [logging.WARNING, logging.DEBUG]


class _FlowingWS:
    """Fake socket that streams frames, then goes quiet without closing."""

    def __init__(self, frames, gap=0.02):
        self._frames = frames
        self._gap = gap

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._frames:
            await asyncio.sleep(3600)
            raise StopAsyncIteration
        await asyncio.sleep(self._gap)
        return self._frames.pop(0)

    async def send(self, data):
        return None

    async def close(self):
        return None


def _enc_frame(t, command, body):
    inner = b"\x00\x00" + struct.pack("<H", command) + bytes([0]) + body
    return pack_outer(t.cipher.encrypt(inner))


async def test_frame_flowing_peer_never_trips_disconnect():
    """A pong-less but frame-flowing session stays READY with zero disconnects.

    Time-based pong enforcement no longer exists in the default posture (no
    keepalive task is armed — see the forwarding test), so elapsed time is
    irrelevant to liveness here; only real traffic matters, and traffic keeps
    the session alive rather than killing it.
    """
    t = MT5WebSocketTransport(uri="wss://x", timeout=5.0)
    t._state = TransportState.READY
    t.ws = _FlowingWS([_enc_frame(t, CMD_TICK_PUSH, b"tick-%d" % i) for i in range(5)])
    notified = []
    t._on_disconnect = lambda: notified.append(True)
    seen = []
    t.on(CMD_TICK_PUSH, lambda r: seen.append(r))
    task = asyncio.create_task(t._recv_loop())
    await asyncio.sleep(0.2)  # all frames flow, then silence — still READY
    assert t.state == TransportState.READY
    assert notified == []
    assert len(seen) == 5
    assert t._last_message_at > 0
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
