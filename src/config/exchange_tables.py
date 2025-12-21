from src.config.exchange import Exchange

_common_tables = {"ohlcv_daily": "ohlcv_daily", "ohlcv_failed": "ohlcv_failed"}

EXCHG_TABLES = {
    Exchange.NSE: {
        **_common_tables,
        "industry_classification": "industry_classification",
    },
    Exchange.NYSE: {**_common_tables},
    Exchange.NASDAQ: {**_common_tables},
}
