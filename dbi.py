from abc import ABCMeta, abstractmethod

class Dbi(metaclass=ABCMeta):
    @abstractmethod
    def record_ticker_change(self, currency_pair, last, lowestAsk, highestBid, percentChange, baseVolume,
                             quoteVolume, isFrozen, lastDayHigh, lastDayLow):
        pass

    @abstractmethod
    def record_order_modified(self, seq, exchange, event_type, rate, amount):
        pass

    @abstractmethod
    def record_order_removed(self, seq, exchange, event_type, rate):
        pass

    @abstractmethod
    def record_new_trade(self, seq, exchange, event_type, rate, amount, date, total, tradeID):
        pass
