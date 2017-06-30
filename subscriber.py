from autobahn.asyncio.wamp import ApplicationSession
from autobahn.asyncio.websocket import WampWebSocketClientFactory
from autobahn.wamp.types import ComponentConfig
from autobahn.websocket.util import parse_url as parse_ws_url
from autobahn.websocket.compress import PerMessageDeflateOffer, \
    PerMessageDeflateResponse, PerMessageDeflateResponseAccept

from sqlite_dbi import SqliteDbi

import asyncio
import txaio
txaio.use_asyncio()  # noqa
import signal
import sys

# The amount for a certain rate on the exchange has changed
ORDER_BOOK_MODIFY = 'orderBookModify'
# There is no more order for the specified rate
ORDER_BOOK_REMOVE = 'orderBookRemove'
# A trade has executed on the exchange
NEW_TRADE = 'newTrade'

def getOnExchangeHandler(exchange, dbi):
    """Returns a handler which logs events on the given exchange and writes them to the database."""
    def onExchange(*events, seq):
        log_message_arr = []
        log_message_base = "[Exchange: {}, seq: {}] ".format(exchange, seq)

        for event in events:
            event_type = event['type']
            data = event['data']
            if event_type == ORDER_BOOK_MODIFY:
                log_message_arr.append(log_message_base + "{type} at rate {rate} now at amount "
                                       "{amount}".format(**data))
                dbi.record_order_modified(seq, exchange, event_type, data['rate'], data['amount'])
            elif event_type == ORDER_BOOK_REMOVE:
                log_message_arr.append(log_message_base + "{type} at rate {rate} has been "
                                       "withdrawn".format(**data))
                dbi.record_order_removed(seq, exchange, event_type, data['rate'])
            elif event_type == NEW_TRADE:
                log_message_arr.append(log_message_base + "{type} trade completed at rate {rate} "
                                       "for amount {amount} (date: {date}, total: {total}, trade "
                                       "ID: {tradeID})".format(**data))
                dbi.record_new_trade(seq, exchange, event_type, data['rate'], data['amount'],
                                     data['date'], data['total'], data['tradeID'])
            else:
                log_message_arr.append(log_message_base + "Unexpected type {}".format(event_type))
        print("\n".join(log_message_arr))
    return onExchange

def getOnTickerHandler(dbi):
    """Returns a handler which logs ticker events and writes them to the provided database."""
    def onTicker(currencyPair, last, lowestAsk, highestBid, percentChange, baseVolume, quoteVolume, isFrozen, lastDayHigh, lastDayLow):
        log_message = """Ticker event received for currency pair {currencyPair}:
- Last:           {last}
- Lowest ask:     {lowestAsk}
- Highest bid:    {highestBid}
- Percent change: {percentChange}
- Base volume:    {baseVolume}
- Quote volume:   {quoteVolume}
- Is Frozen?      {isFrozen}
- 24 hour high:   {lastDayHigh}
- 24 hour low:    {lastDayLow}""".format(
            currencyPair = currencyPair,
            last = last,
            lowestAsk = lowestAsk,
            highestBid = highestBid,
            percentChange = percentChange,
            baseVolume = baseVolume,
            quoteVolume = quoteVolume,
            isFrozen = isFrozen,
            lastDayHigh = lastDayHigh,
            lastDayLow = lastDayLow,
        )
        print(log_message)
        dbi.record_ticker_change(currencyPair, last, lowestAsk, highestBid, percentChange,
                                 baseVolume, quoteVolume, isFrozen, lastDayHigh, lastDayLow)
    return onTicker


class PoloniexComponent(ApplicationSession):
    def onConnect(self):
        print("Establishing connection to WAMP server")
        self.join(self.config.realm)

    @asyncio.coroutine
    def __trySubscribe(self, func, channel):
        try:
            yield from self.subscribe(func, channel)
        except Exception as e:
            print("Could not subscribe to topic {}".format(channel), e)

    @asyncio.coroutine
    def onJoin(self, details):
        dbi = SqliteDbi()
        yield from self.__trySubscribe(getOnTickerHandler(dbi), 'ticker')
        yield from self.__trySubscribe(getOnExchangeHandler('BTC_ETH', dbi), 'BTC_ETH')


def main():
    url = "wss://api.poloniex.com:443"
    realm = "realm1"

    log = txaio.make_logger()
    log_level = 'info' # To see more detail when establishing the connection, set this to 'debug'

    def create():
        cfg = ComponentConfig(realm, dict())
        try:
            session = PoloniexComponent(cfg)
        except Exception as e:
            log.error('ApplicationSession could not be instantiated: {}'.format(e))
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.stop()
            raise
        else:
            return session

    # try to parse WebSocket URL ..
    isSecure, host, port, _, _, _ = parse_ws_url(url)

    # create a WAMP-over-WebSocket transport client factory
    transport_factory = WampWebSocketClientFactory(create, url=url)

    # The permessage-deflate extensions offered to the server ..
    offers = [PerMessageDeflateOffer()]

    # Function to accept permessage_delate responses from the server ..
    def accept(response):
        if isinstance(response, PerMessageDeflateResponse):
            return PerMessageDeflateResponseAccept(response)

    # set WebSocket options for all client connections
    # NOTE: Here we use an openHandshakeTimeout of 25 seconds rather than the default of 2.5
    # seconds. The default is not suitable for Poloniex.
    transport_factory.setProtocolOptions(maxFramePayloadSize=1048576,
                                         maxMessagePayloadSize=1048576,
                                         autoFragmentSize=65536,
                                         failByDrop=False,
                                         openHandshakeTimeout=25,
                                         closeHandshakeTimeout=1.,
                                         tcpNoDelay=True,
                                         autoPingInterval=10.,
                                         autoPingTimeout=5.,
                                         autoPingSize=4,
                                         perMessageCompressionOffers=offers,
                                         perMessageCompressionAccept=accept)
    # SSL context for client connection
    ssl = isSecure

    # start the client connection
    loop = asyncio.get_event_loop()
    txaio.config.loop = loop
    coro = loop.create_connection(transport_factory, host, port, ssl=ssl)

    (transport, protocol) = loop.run_until_complete(coro)

    # start logging
    txaio.start_logging(level=log_level)

    try:
        loop.add_signal_handler(signal.SIGTERM, loop.stop)
    except NotImplementedError:
        # signals are not available on Windows
        pass

    # 4) now enter the asyncio event loop
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        # wait until we send Goodbye if user hit ctrl-c
        # (done outside this except so SIGTERM gets the same handling)
        pass

    # give Goodbye message a chance to go through, if we still
    # have an active session
    if protocol._session:
        loop.run_until_complete(protocol._session.leave())

    loop.close()

if __name__ == "__main__":
    main()
