import sqlite3

from dbi import Dbi

ORDER_BOOK_TABLE_DEF = """
CREATE TABLE IF NOT EXISTS orderBook (
  seq integer,
  exchange text,
  type text,
  rate real,
  amount real,
  date text,
  total real,
  tradeID text,
  isTrade integer,
  isCancelled integer
)
"""

INSERT_INTO_ORDER_BOOK_STATEMENT = """
INSERT INTO orderBook
  (seq, exchange, type, rate, amount, date, total, tradeID, isTrade, isCancelled)
VALUES
  (  ?,        ?,    ?,    ?,      ?,    ?,     ?,       ?,       ?,           ?)
"""

TICKER_TABLE_DEF = """
CREATE TABLE IF NOT EXISTS ticker (
  currencyPair text,
  last real,
  lowestAsk real,
  highestBid real,
  percentChange real,
  baseVolume real,
  quoteVolume real,
  isFrozen integer,
  lastDayHigh real,
  lastDayLow real
)
"""

INSERT_INTO_TICKER_STATEMENT = """
INSERT INTO ticker
  (currencyPair, last, lowestAsk, highestBid, percentChange, baseVolume, quoteVolume, isFrozen,
   lastDayHigh, lastDayLow)
VALUES
  (           ?,    ?,         ?,          ?,             ?,          ?,           ?,        ?,
             ?,          ?)
"""

class SqliteDbi(Dbi):
    def __init__(self):
        self.__connection = sqlite3.connect('poloniex.db')
        cursor = self.__connection.cursor()
        cursor.execute(ORDER_BOOK_TABLE_DEF)
        cursor.execute(TICKER_TABLE_DEF)
        self.__connection.commit()

    def record_ticker_change(self, currency_pair, last, lowestAsk, highestBid, percentChange, baseVolume,
                             quoteVolume, isFrozen, lastDayHigh, lastDayLow):
        cursor = self.__connection.cursor()
        cursor.execute(
            INSERT_INTO_TICKER_STATEMENT,
            (currency_pair, last, lowestAsk, highestBid, percentChange, baseVolume, quoteVolume,
             isFrozen, lastDayHigh, lastDayLow)
        )
        self.__connection.commit()

    def record_order_modified(self, seq, exchange, event_type, rate, amount):
        cursor = self.__connection.cursor()
        cursor.execute(
            INSERT_INTO_ORDER_BOOK_STATEMENT,
            (seq, exchange, event_type, rate, amount, '', 0, '', 0, 0)
        )
        self.__connection.commit()

    def record_order_removed(self, seq, exchange, event_type, rate):
        cursor = self.__connection.cursor()
        cursor.execute(
            INSERT_INTO_ORDER_BOOK_STATEMENT,
            (seq, exchange, event_type, rate, 0, '', 0, '', 0, 1)
        )
        self.__connection.commit()

    def record_new_trade(self, seq, exchange, event_type, rate, amount, date, total, tradeID):
        cursor = self.__connection.cursor()
        cursor.execute(
            INSERT_INTO_ORDER_BOOK_STATEMENT,
            (seq, exchange, event_type, rate, amount, date, total, tradeID, 1, 0)
        )
        self.__connection.commit()
