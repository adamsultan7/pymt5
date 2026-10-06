Quick Start
===========

Sync API (recommended for trading bots)
---------------------------------------

No asyncio in user code — see ``examples/09_sync_bot.py`` for the full
recipe (subscribe, place, watch fills, survive a disconnect).

.. code-block:: python

   from pymt5.sync import SyncMT5Client

   with SyncMT5Client(auto_reconnect=True) as client:
       client.login(login=12345678, password="your-password")
       client.ensure_market_data(["EURUSD"])
       ticket = client.place_market("EURUSD", "buy", 0.01)
       print(ticket, client.connection_stats())

Reconnect exhaustion: ``max_reconnect_attempts=0``/``None`` retries forever
with capped backoff. After a finite round is exhausted, the next public call
triggers a fresh reconnect round (lazy recovery) instead of failing forever.

Basic Connection
----------------

.. code-block:: python

   import asyncio
   from pymt5 import MT5WebClient


   async def main():
       async with MT5WebClient(auto_reconnect=True) as client:
           await client.login(login=12345678, password="your-password")

           # Load symbol cache
           await client.load_symbols()
           print(f"Loaded {len(client.symbol_names)} symbols")

           # Full account info
           acct = await client.get_account()
           print(f"Balance: {acct['balance']}, Currency: {acct['currency']}")

           # Symbol groups
           groups = await client.get_symbol_groups()
           print(f"Groups: {groups}")

           await asyncio.sleep(5)


   asyncio.run(main())

Tick Subscription
-----------------

.. code-block:: python

   async with MT5WebClient() as client:
       await client.login(login=12345678, password="your-password")
       await client.load_symbols()

       def on_ticks(ticks):
           for t in ticks:
               print(f"TICK {t.get('symbol', t['symbol_id'])}: "
                     f"bid={t['bid']} ask={t['ask']}")

       client.on_tick(on_ticks)
       await client.subscribe_symbols(["EURUSD", "GBPUSD"])
       await asyncio.sleep(30)

Logging
-------

Enable debug logging to see protocol details:

.. code-block:: python

   import logging
   logging.basicConfig(level=logging.DEBUG)
   # Loggers: pymt5.client, pymt5.transport
