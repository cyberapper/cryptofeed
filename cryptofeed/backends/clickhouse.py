from cryptofeed.backends.backend import BackendBookCallback, BackendCallback, BackendQueue
import clickhouse_connect

from collections import defaultdict
from datetime import timezone, datetime as dt
from typing import Tuple
from cryptofeed.defines import CANDLES, FUNDING, OPEN_INTEREST, TICKER, TRADES, LIQUIDATIONS, INDEX
import json


class ClickhouseCallback(BackendQueue):
    def __init__(self, host='127.0.0.1', user=None, pw=None, db=None, port=None, table=None, none_to=None, numeric_type=float, **kwargs):
        """
        host: str
            Database host address
        user: str
            The name of the database role used for authentication.
        db: str
            The name of the database to connect to.
        pw: str
            Password to be used for authentication, if the server requires one.
        table: str
            Table name to insert into. Defaults to default_table that should be specified in child class
        """
        self.conn = None
        self.table = table if table else self.default_table
        self.user = user
        self.pw = pw
        self.db = db
        self.host = host
        self.port = port
        self.running = True

        self.numeric_type = numeric_type
        self.none_to = none_to

    def _connect(self):
        if self.conn is None:
            self.conn = clickhouse_connect.get_client(host=self.host, username=self.user, password=self.pw, database=self.db, port=self.port)

    def format(self, data: Tuple):
        exchange, symbol, timestamp, receipt_timestamp, raw = data

        formatted_data = [exchange, symbol, timestamp, receipt_timestamp]
        formatted_data.extend((raw.values()))
        return formatted_data

    async def writer(self):
        while self.running:
            async with self.read_queue() as updates:
                if len(updates) > 0:
                    batch = []
                    for data in updates:
                        print(data)
                        ts = dt.fromtimestamp(data['timestamp'], tz=timezone.utc) if data['timestamp'] else None
                        rts = dt.fromtimestamp(data['receipt_timestamp'], tz=timezone.utc) if data['receipt_timestamp'] else None
                        batch.append((data['exchange'], data['symbol'], ts, rts, data))

                    self.write_batch(batch)

    def write_batch(self, updates: list):
        self._connect()
        rows = [self.format(u) for u in updates]
        insert_context = self.conn.create_insert_context(table=self.table, data=rows)
        try:
            self.conn.insert(table=self.table, context=insert_context)
        except Exception:
            pass


class TradeClickhouse(ClickhouseCallback, BackendCallback):
    default_table = TRADES

    def format(self, data: Tuple):
        exchange, symbol, timestamp, receipt_timestamp, raw = data
        return [timestamp, receipt_timestamp, exchange, symbol, raw['side'], raw['amount'], raw['price'], raw['id'], raw['type']]


class FundingClickhouse(ClickhouseCallback, BackendCallback):
    default_table = FUNDING

    def format(self, data: Tuple):
        exchange, symbol, timestamp, receipt_timestamp, raw = data

        mark_price = raw['mark_price'] if raw['mark_price'] else None
        next_funding_time = dt.fromtimestamp(raw['next_funding_time'], tz=timezone.utc) if raw['next_funding_time'] else None
        predicted_rate = raw['predicted_rate'] if raw['predicted_rate'] else None

        return [timestamp, receipt_timestamp, exchange, symbol, mark_price, raw['rate'], next_funding_time, predicted_rate]


class TickerClickhouse(ClickhouseCallback, BackendCallback):
    default_table = TICKER

    def format(self, data: Tuple):
        exchange, symbol, timestamp, receipt_timestamp, raw = data
        return [timestamp, receipt_timestamp, exchange, symbol, raw['bid'], raw['ask']]


class OpenInterestClickhouse(ClickhouseCallback, BackendCallback):
    default_table = OPEN_INTEREST

    def format(self, data: Tuple):
        exchange, symbol, timestamp, receipt_timestamp, raw = data
        return [timestamp, receipt_timestamp, exchange, symbol, raw['open_interest']]


class IndexClickhouse(ClickhouseCallback, BackendCallback):
    default_table = INDEX

    def format(self, data: Tuple):
        exchange, symbol, timestamp, receipt_timestamp, raw = data
        return [timestamp, receipt_timestamp, exchange, symbol, raw['price']]


class LiquidationsClickhouse(ClickhouseCallback, BackendCallback):
    default_table = LIQUIDATIONS

    def format(self, data: Tuple):
        exchange, symbol, timestamp, receipt_timestamp, raw = data
        return [timestamp, receipt_timestamp, exchange, symbol, raw['side'], raw['quantity'], raw['price'], raw['id'], raw['status']]


class BookClickhouse(ClickhouseCallback, BackendBookCallback):
    default_key = 'book'

    def __init__(self, *args, snapshots_only=False, snapshot_interval=1000, **kwargs):
        self.snapshots_only = snapshots_only
        self.snapshot_interval = snapshot_interval
        self.snapshot_count = defaultdict(int)
        super().__init__(*args, **kwargs)

    def format(self, data: Tuple):
        exchange, symbol, timestamp, receipt_timestamp, raw = data
        if 'book' in raw:
            raw = {'snapshot': raw['book']}
        else:
            raw = {'delta': raw['delta']}

        return [timestamp, receipt_timestamp, exchange, symbol, json.dumps(raw)]


class CandleClickhouse(ClickhouseCallback, BackendCallback):
    default_table = CANDLES

    def format(self, data: Tuple):
        exchange, symbol, timestamp, receipt_timestamp, raw = data

        candle_start = dt.fromtimestamp(raw['start'], tz=timezone.utc)
        candle_stop = dt.fromtimestamp(raw['stop'], tz=timezone.utc)

        return [timestamp, receipt_timestamp, exchange, symbol, candle_start, candle_stop, raw['interval'], raw['trades'], raw['open'], raw['close'], raw['high'], raw['low'], raw['volume'], raw['closed']]
