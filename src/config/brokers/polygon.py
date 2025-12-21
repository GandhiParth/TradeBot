from pathlib import Path

from src.config.exchange import Exchange
from src.config.market import Market


class PolygonConfig:
    NAME = "POLYGON"

    SUPPORTED = {Market.US_EQUITIES: {Exchange.NYSE, Exchange.NASDAQ}}

    CREDENTIALS_PATH = Path("/home/parthgandhi/.conf/credentials/polygon.ini")

    LOOKBACK_DAYS_LIMIT = 365 * 2

    API_RATE_LIMIT_SECONDS = {
        "calls": 5,
        "period": 1,
    }
