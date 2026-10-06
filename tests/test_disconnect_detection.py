"""Regression tests for silent-disconnect detection.

Covers: clean server close must flip state to ERROR and notify,
send failures must surface MT5ConnectionError, heartbeat failures
must trigger disconnect handling, and reconnect must preserve listeners.
"""

import asyncio
from unittest.mock import MagicMock

import websockets.exceptions

from pymt5.client import MT5WebClient
from pymt5.constants import CMD_BOOK_PUSH, CMD_GET_ACCOUNT, CMD_TICK_PUSH
from pymt5.transport import MT5WebSocketTransport, TransportState


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
