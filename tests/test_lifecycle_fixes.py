"""Regression tests for connect/lifecycle/pool fixes."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from pymt5._pool import MT5ConnectionPool
from pymt5.client import MT5WebClient
from pymt5.transport import MT5WebSocketTransport, TransportState


async def test_connect_bootstrap_failure_cleans_up():
    t = MT5WebSocketTransport(uri="wss://x", timeout=5.0)
    mock_ws = MagicMock()
    mock_ws.send = AsyncMock()
    mock_ws.close = AsyncMock()
    mock_ws.__aiter__ = MagicMock(return_value=iter([]))

    async def _bad_bootstrap(cmd, payload=b""):
        from pymt5.transport import CommandResult

        return CommandResult(command=cmd, code=1, body=b"x" * 100)

    with patch(
        "pymt5.transport._ws_async_client.connect",
        new_callable=AsyncMock,
        return_value=mock_ws,
    ):
        t._send_raw = _bad_bootstrap  # type: ignore[method-assign]
        try:
            await t.connect()
            raise AssertionError("should raise")
        except Exception:
            pass
    assert t.ws is None
    assert t._recv_task is None
    assert t.state == TransportState.ERROR


async def test_logout_clears_credentials_and_logged_in_on_failure():
    import pytest

    client = MT5WebClient()
    client._logged_in = True
    client._login_kwargs = {"login": 1, "password": "pw"}
    client.transport.send_command = AsyncMock(side_effect=RuntimeError("dead"))
    with pytest.raises(RuntimeError, match="dead"):
        await client.logout()
    assert client._logged_in is False
    assert client._login_kwargs is None


async def test_start_heartbeat_restarts_done_task():
    client = MT5WebClient(heartbeat_interval=999)
    client.transport.send_command = AsyncMock()
    client._start_heartbeat()
    first = client._heartbeat_task
    assert first is not None
    first.cancel()
    try:
        await first
    except (asyncio.CancelledError, Exception):
        pass
    client._start_heartbeat()
    assert client._heartbeat_task is not first
    client._stop_heartbeat()


async def test_pool_connect_one_closes_on_login_failure():
    from pymt5._pool import PoolAccount

    pool = MT5ConnectionPool([PoolAccount(server="wss://x", login=1, password="pw")])
    with patch("pymt5._pool.MT5WebClient") as mock_cls:
        inst = MagicMock()
        inst.connect = AsyncMock()
        inst.login = AsyncMock(side_effect=RuntimeError("bad pw"))
        inst.close = AsyncMock()
        mock_cls.return_value = inst
        try:
            await pool._connect_one(pool._accounts[0])
            raise AssertionError("should raise")
        except RuntimeError:
            pass
        inst.close.assert_awaited_once()
        assert 1 not in pool._clients


async def test_handle_disconnect_user_callback_error_still_reconnects():
    client = MT5WebClient(auto_reconnect=True)
    client._login_kwargs = {"login": 1, "password": "x"}

    def _bad():
        raise ValueError("cb boom")

    client.on_disconnect(_bad)
    client._handle_disconnect()
    assert client._reconnect_task is not None
    client._reconnect_task.cancel()
    try:
        await client._reconnect_task
    except (asyncio.CancelledError, Exception):
        pass
