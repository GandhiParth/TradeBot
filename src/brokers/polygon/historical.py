import logging

import polars as pl
from massive import RESTClient

from src.brokers.polygon.api import get_date_range_grouped_daily_aggs

logger = logging.getLogger(__name__)


def polygon_historical(
    client: RESTClient,
    file_location: str,
    start_date: str,
    end_date: str,
    db_conn: str,
    insert_table_name: str,
):
    data = get_date_range_grouped_daily_aggs(
        client=client, start_date=start_date, end_date=end_date
    )

    logger.info(
        f"# Fetched Data for {data.select(pl.col('symbol').unique()).shape[0]} symbols"
    )

    symbols_list = pl.read_parquet(file_location).get_column("symbol").to_list()

    data = data.filter(pl.col("symbol").is_in(symbols_list))

    logger.info(
        f"# Symbols after filtering: {data.select(pl.col('symbol').unique()).shape[0]}"
    )

    data.write_database(table_name=insert_table_name, connection=db_conn)

    logger.info("Data Inserted Successfully")
