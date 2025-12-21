from pathlib import Path
from src.config.market import Market
from src.config.exchange import Exchange


class PolygonConfig:
    NAME = "POLYGON"

    SUPPORTED_MARKETS = {Market.US_EQUITIES}
    SUPPROTED_EXCHANGES = {Exchange.NYSE, Exchange.NASDAQ}

    CREDENTIALS_PATH = Path("/home/parthgandhi/conf/credentials/polygon.ini")

    LOOKBACK_DAYS_LIMIT = 365 * 2

    API_RATE_LIMIT_SECONDS = {
        "calls": 5,
        "period": 1,
    }
