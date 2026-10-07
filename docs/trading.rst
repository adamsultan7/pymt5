Trading
=======

pymt5 provides both low-level and high-level trading interfaces.

High-Level Helpers
------------------

All high-level helpers auto-resolve ``digits`` from the symbol cache (call
``load_symbols()`` first) and convert lot volume to MT5 integer format.

Market Orders
~~~~~~~~~~~~~

.. code-block:: python

   # Market buy 0.01 lots of EURUSD with SL/TP
   result = await client.buy_market("EURUSD", 0.01, sl=1.0800, tp=1.1200)

   # Market sell 0.05 lots with custom deviation
   result = await client.sell_market("EURUSD", 0.05, deviation=30)

   # Check result
   if result.success:
       print(f"Order placed: deal={result.deal}, price={result.price}")
   else:
       print(f"Failed: {result.description}")

Pending Orders
~~~~~~~~~~~~~~

.. code-block:: python

   # Buy limit
   result = await client.buy_limit("EURUSD", 0.1, price=1.0800,
                                    sl=1.0750, tp=1.0900)

   # Sell stop
   result = await client.sell_stop("GBPUSD", 0.1, price=1.2500)

   # Buy stop-limit (trigger at price, then place limit at stop_limit_price)
   result = await client.buy_stop_limit("EURUSD", 0.1, price=1.1000,
                                         stop_limit_price=1.0950,
                                         sl=1.0900, tp=1.1100)

Position Management
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Close position (auto-detects BUY/SELL direction)
   result = await client.close_position("EURUSD", position_id=123456,
                                         volume=0.1)

   # Close by opposite position (hedge netting)
   result = await client.close_position_by("EURUSD", position_id=123456,
                                            position_by=789012)

   # Modify SL/TP
   result = await client.modify_position_sltp("EURUSD", position_id=123456,
                                               sl=1.0850, tp=1.0950)

   # Cancel pending order
   result = await client.cancel_pending_order(order=789012)

Ticket-Based Trading
~~~~~~~~~~~~~~~~~~~~

``place_market()`` returns the position ticket directly instead of a result
object, and ``close_position_by_ticket()`` / ``modify_sltp()`` work from the
ticket alone (no symbol/volume/direction bookkeeping by the caller):

.. code-block:: python

   ticket = await client.place_market("EURUSD", "buy", 0.01, sl=1.0800, tp=1.1200)
   closing_deal = await client.close_position_by_ticket(ticket)
   result = await client.modify_sltp(ticket, sl=1.0850, tp=1.0950)
   reason, deal = await client.get_close_reason(ticket)

Fill confirmation waits on the server push (cmd-19) correlated by action id,
falling back to the cmd-12 response tickets — mirroring the official Web
Terminal, which likewise waits on the push with no fixed timer (the 5s/15s
transport watchdog bounds the wait instead). The ``fill_timeout`` default is
30.0s so slow fills inside the UI's own ~7s requote window are not
misreported; pass an explicitly short ``fill_timeout`` to fail faster. Every
expiry path fails closed with no resend — reconcile with ``positions_get()``.

A push that arrives with zero tickets is proof the server answered without
executing: it raises ``TradeError`` (with the push's real response retcode —
never the push serial), never ``MT5TimeoutError``. ``MT5TimeoutError`` now
means only "no final push within budget" — genuinely ambiguous, reconcile
required. Downstream code that catches ``MT5TimeoutError`` to reconcile
should also catch ``TradeError`` for the immediate-reject case, where a
reprice/retry is safe without reconciliation.

Closing an already-closed position raises
:class:`~pymt5.PositionAlreadyClosedError`, a subclass of ``TradeError``.
Closing is idempotent, so bots should purge local state instead of retrying:

.. code-block:: python

   from pymt5 import MT5TimeoutError, PositionAlreadyClosedError, TradeError

   try:
       await client.close_position_by_ticket(ticket)
       purge(ticket)
   except PositionAlreadyClosedError:
       purge(ticket)  # already flat: desired end-state, do not retry
   except TradeError as exc:
       log.warning("close rejected: retcode=%s", exc.retcode)  # safe to reprice/retry
   except MT5TimeoutError:
       reconcile_with_positions_get()  # ambiguous: verify before any resend

.. note::
   Migration: the ``fill_timeout`` default changed from 10.0 to 30.0. Callers
   that relied on a fast ``MT5TimeoutError`` should pass an explicit short
   timeout.

Build 6090 wire note: market deals are sent with ``trade_action=3``
(``TRADE_ACTION_MARKET_DEAL``) because 6090+ servers drop or reject the
older ``action=1`` (``TRADE_ACTION_DEAL``) for market opens/closes. The
Python API keeps accepting ``TRADE_ACTION_DEAL`` unchanged — only the wire
value is translated, at the single ``trade_request()`` serialization point.
The official UI actually selects an execution mode per symbol (0..4);
unconditional 3 mirrors proven production behavior on Pepperstone/MetaQuotes
demo servers, though exchange-execution symbols may later need the
per-symbol ``trade_exemode``.

Fill mode is auto-detected from the full symbol spec (``trade_fill_flags``:
FOK-only symbols get ``ORDER_FILLING_FOK``, IOC-only symbols get
``ORDER_FILLING_IOC``), pass ``filling=`` explicitly to override — including
on ``close_position_by_ticket()``, which previously had no override. On
``10030`` (invalid filling) the request is retried **once** with the
alternate mode inside the same ``fill_timeout`` budget, then fails closed.
Strict brokers reject the wrong guess live (FOK sent to an IOC-only symbol),
so a close that previously died on 10030 now succeeds on the second attempt
with no caller change; two consecutive 10030s raise ``TradeError``.

On requote (retcode 10004) for market orders, ``place_market()`` can retry
automatically with ``requote_retries`` (default 0 = raise immediately).
Each retry waits ``requote_delay`` seconds (default: the symbol's
``trade.lf`` field, else 7.0s), refreshes the order price from the requote
payload quotes, and resends — all inside the same overall ``fill_timeout``
budget, so set the budget above ``requote_delay * (retries + 1)``. Pending
orders and position closes never auto-retry.

Low-Level Trade Request
-----------------------

For full control over all trade fields, use ``trade_request()`` directly:

.. code-block:: python

   from pymt5 import (TRADE_ACTION_DEAL, ORDER_TYPE_BUY,
                       ORDER_FILLING_IOC)

   result = await client.trade_request(
       trade_action=TRADE_ACTION_DEAL,
       symbol="EURUSD",
       volume=client._volume_to_lots(0.01),
       digits=5,
       trade_type=ORDER_TYPE_BUY,
       type_filling=ORDER_FILLING_IOC,
       deviation=20,
       comment="my trade",
   )

TradeResult
-----------

All trade methods return a :class:`~pymt5.TradeResult` dataclass:

- ``retcode`` — MT5 return code (e.g. 10009 = done)
- ``description`` — human-readable description
- ``success`` — ``True`` if retcode indicates success
- ``deal`` — deal ticket number
- ``order`` — order ticket number
- ``volume`` — executed volume (MT5 integer format)
- ``price`` — execution price
- ``bid`` / ``ask`` — market prices at execution
- ``comment`` — server comment
- ``request_id`` — request identifier
- ``elapsed_ms`` — milliseconds from send to terminal outcome
