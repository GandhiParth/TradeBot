from pathlib import Path

from src.config.exchange import Exchange
from src.config.market import Market


class KiteConfig:
    NAME = "KITE"

    SUPPORTED_MARKETS = {Market.INDIA_EQUITIES: {Exchange.NSE}}

    CREDENTIALS_PATH = Path("/home/parthgandhi/.conf/credentials/kite.ini")

    LOOKBACK_DAYS_LIMIT = None

    HISTORICAL_DATA_LIMIT_DAYS = {
        "minute": 30,
        "3minute": 90,
        "5minute": 90,
        "10minute": 90,
        "15minute": 180,
        "30minute": 180,
        "60minute": 365,
        "day": 2000,
    }

    API_RATE_LIMIT_SECONDS = {"quote": 1, "historical": 3, "order": 10, "others": 10}

    TICKER_LIMIT = {"max_tokens": 3000}
