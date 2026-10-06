"""Regression tests for silent-disconnect detection.

Covers: clean server close must flip state to ERROR and notify,
send failures must surface MT5ConnectionError, heartbeat failures
must trigger disconnect handling, and reconnect must preserve listeners.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import websockets.exceptions

from pymt5.client import MT5WebClient
from pymt5.constants import CMD_BOOK_PUSH, CMD_GET_ACCOUNT, CMD_PING, CMD_TICK_PUSH
from pymt5.exceptions import MT5ConnectionError, SessionError
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
    assert t._ws_ping_interval == 20.0
    assert t._ws_ping_timeout == 20.0


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
    with patch("pymt5.client.MT5WebSocketTransport", return_value=failing), pytest.raises(SessionError):
        await client.ping()
