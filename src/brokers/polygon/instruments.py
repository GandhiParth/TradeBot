import logging

import polars as pl
from massive import RESTClient

from src.brokers.polygon.api import get_tickers

logger = logging.getLogger(__name__)


def fetch_instruments(
    client: RESTClient, type: str, market: str, exchange: str
) -> pl.DataFrame:
    out = get_tickers(client=client, type=type, market=market, exchange=exchange)
    logger.info(f"Instruments fetched for exchange: {exchange}")
    return out
