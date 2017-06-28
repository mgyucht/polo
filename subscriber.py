from autobahn.asyncio.wamp import ApplicationSession
from autobahn.asyncio.websocket import WampWebSocketClientFactory
from autobahn.wamp.types import ComponentConfig
from autobahn.websocket.util import parse_url as parse_ws_url
from autobahn.websocket.compress import PerMessageDeflateOffer, \
    PerMessageDeflateResponse, PerMessageDeflateResponseAccept

import asyncio
import txaio
txaio.use_asyncio()  # noqa
import signal


class PoloniexComponent(ApplicationSession):
    def onConnect(self):
        print("Establishing connection to WAMP server")
        self.join(self.config.realm)

    @asyncio.coroutine
    def onJoin(self, details):
        def onTicker(*args):
            print("Ticker event received:", args)

        try:
            yield from self.subscribe(onTicker, 'ticker')
        except Exception as e:
            print("Could not subscribe to topic:", e)


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
