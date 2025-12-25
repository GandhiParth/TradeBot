from src.config.exchange import Exchange
from src.config.market import Market

MARKET_EXCHANGE = {
    Market.INDIA_EQUITIES: {
        Exchange.NSE,
    },
    Market.US_EQUITIES: {
        Exchange.NYSE,
        Exchange.NASDAQ,
    },
    Market.INDIA: {Exchange.NSE},
}
