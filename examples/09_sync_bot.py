"""
Example 09: Sync end-to-end bot recipe (the documented default entry point).

Demonstrates the full bot loop with zero asyncio in user code:
  - Connect + login with auto-reconnect enabled
  - ensure_market_data() to resolve names and subscribe ticks
  - Watch TradeResultEvent pushes for async fill notifications
  - place_market() returning a position ticket (no action-id bookkeeping)
  - get_close_reason() after closing the position
  - connection_stats() for observability

Disconnect survival:
  - Short outages heal in the background via the reconnect loop.
  - If a round exhausts max_reconnect_attempts, the transport sits in ERROR
    and the NEXT call automatically triggers a fresh reconnect round (lazy
    recovery) instead of raising forever. Set max_reconnect_attempts=0/None
    to retry forever with capped backoff.

Credentials come from the environment (never hardcode them):
  MT5_SERVER, MT5_LOGIN, MT5_PASSWORD
"""

import logging
import os
import time

os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("no_proxy", "*")

from pymt5 import MT5ConnectionError, PyMT5Error, TradeError
from pymt5.sync import SyncMT5Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("example09")

SERVER = os.getenv("MT5_SERVER", "wss://web.metatrader.app/terminal")
LOGIN = int(os.getenv("MT5_LOGIN", "0") or 0)
PASSWORD = os.getenv("MT5_PASSWORD", "")
SYMBOL = os.getenv("MT5_SYMBOL", "EURUSD")


def main():
    if not LOGIN or not PASSWORD:
        raise SystemExit("set MT5_LOGIN and MT5_PASSWORD in the environment")

    with SyncMT5Client(uri=SERVER, auto_reconnect=True, timeout=30) as client:
        client.login(login=LOGIN, password=PASSWORD)
        log.info("Logged in (server_build=%d)", client.server_build)

        # Resolve the broker-suffixed name and subscribe in one call.
        ids = client.ensure_market_data([SYMBOL])
        log.info("Market data ensured: %s", ids)

        # Watch async fill notifications (runs on the background loop thread:
        # keep the callback fast and thread-safe).
        fills = []

        def on_fill(event):
            fills.append(event)
            log.info(
                "  [PUSH] fill: retcode=%d order=%d deal=%d price=%s",
                event.retcode,
                event.order,
                event.deal,
                event.price,
            )

        client.on_trade_result_event(on_fill)

        # Place a small market order; returns the position ticket directly.
        try:
            ticket = client.place_market(SYMBOL, "buy", 0.01, comment="sync-bot")
        except TradeError as exc:
            log.error("Order rejected: %s", exc)
            return
        log.info("Opened position ticket=%d", ticket)

        # Give pushes a moment, then report what we observed.
        time.sleep(5)
        log.info("Observed %d fill push(es); stats=%s", len(fills), client.connection_stats())

        # Close and explain the close.
        try:
            closing_deal = client.close_position_by_ticket(ticket, comment="sync-bot-close")
            log.info("Closed ticket=%d with deal=%d", ticket, closing_deal)
        except TradeError as exc:
            log.error("Close rejected: %s", exc)
            return
        reason, deal = client.get_close_reason(ticket)
        log.info("Close reason for %d: %s", ticket, reason)

    log.info("Done")


if __name__ == "__main__":
    try:
        main()
    except MT5ConnectionError as exc:
        log.error("Connection failed: %s", exc)
    except PyMT5Error as exc:
        log.error("MT5 error: %s", exc)
