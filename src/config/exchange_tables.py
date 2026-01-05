from src.config.exchange import Exchange

_common_tables = {
    "equity_ohlcv_daily": "equity_ohlcv_daily",
    "equity_ohlcv_failed": "equity_ohlcv_failed",
}

EXCHG_TABLES = {
    Exchange.NSE: {
        **_common_tables,
        **{
            "indices_ohlcv_daily": "indices_ohlcv_daily",
            "indices_ohlcv_failed": "indices_ohlcv_failed",
        },
    },
    Exchange.NYSE: {**_common_tables},
    Exchange.NASDAQ: {**_common_tables},
}
