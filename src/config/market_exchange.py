from src.config.market import Market
from src.config.exchange import Exchange

MARKET_EXCHANGE = {
    Market.INDIA_EQUITIES: {
        Exchange.NSE,
    },
    Market.US_EQUITIES: {
        Exchange.NYSE,
        Exchange.NASDAQ,
    },
}
