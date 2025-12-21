import logging
from datetime import datetime, timedelta, timezone

import polars as pl
from massive import RESTClient
from ratelimit import limits, sleep_and_retry

logger = logging.getLogger(__name__)

CALLS = 5
PERIOD = 60


def get_ticker_types(client: RESTClient, asset_class: str, locale: str) -> pl.DataFrame:
    """ """

    ticker_types = client.get_ticker_types(asset_class=asset_class, locale=locale)

    rows = [
        {
            "asset_class": t.asset_class,
            "code": t.code,
            "description": t.description,
            "locale": t.locale,
        }
        for t in ticker_types
    ]

    df = pl.DataFrame(rows)
    return df


def get_tickers(
    client: RESTClient,
    type: str,
    market: str,
    exchange: str,
    active: str = "true",
    order: str = "asc",
    limit: str = "1000",
) -> pl.DataFrame:
    """ """

    tickers = []
    for t in client.list_tickers(
        type=type,
        market=market,
        exchange=exchange,
        active=active,
        order=order,
        limit=limit,
    ):
        tickers.append(t)

    rows = [
        {
            "active": t.active,
            "cik": t.cik,
            "composite_figi": t.composite_figi,
            "currency_name": t.currency_name,
            "locale": t.locale,
            "market": t.market,
            "name": t.name,
            "exchange": t.primary_exchange,
            "symbol": t.ticker,
        }
        for t in tickers
    ]

    df = pl.DataFrame(rows)
    return df


def date_range(
    start_date: str,
    end_date: str,
    skip_weekends: bool = True,
) -> list[str]:
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    if start > end:
        raise ValueError("start_date must be <= end_date")

    dates = []
    current = start
    while current <= end:
        # weekday(): Mon=0, ..., Sun=6
        if not skip_weekends or current.weekday() < 5:
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_grouped_daily_aggs(
    client: RESTClient,
    date: str,
    **kwargs,
):
    grouped = client.get_grouped_daily_aggs(date=date, **kwargs)

    if grouped == []:
        return None

    rows = [
        {
            "symbol": t.ticker,
            "timestamp": datetime.fromtimestamp(t.timestamp / 1000, tz=timezone.utc),
            "open": t.open,
            "high": t.high,
            "low": t.low,
            "close": t.close,
            "volume": t.volume,
            "vwap": t.vwap,
        }
        for t in grouped
    ]

    df = pl.DataFrame(rows).with_columns(
        pl.col("timestamp")
        .dt.convert_time_zone(time_zone="America/New_York")
        .alias("timestamp")
    )

    return df


def get_date_range_grouped_daily_aggs(
    client: RESTClient, start_date: str, end_date: str, **kwargs
):
    df_list = []
    date_ranges_list = date_range(
        start_date=start_date, end_date=end_date, skip_weekends=True
    )

    for d in date_ranges_list:
        df = get_grouped_daily_aggs(client=client, date=d, **kwargs)

        if (df is None) or (df.is_empty()):
            continue

        df_list.append(df)

        logger.info(f"Fetched Data for DATE: {d}")

    out = pl.concat(df_list, how="vertical_relaxed")

    return out
