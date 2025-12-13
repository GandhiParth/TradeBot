import polars as pl
from src.scans.conf import VOLUME_THRESHOLD, filters_dict
from functools import reduce
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def prep_scan_data(
    ins_file_path: str,
    db_conn: str,
    kite_conf: dict,
    gains_dict: dict = filters_dict,
) -> pl.LazyFrame:

    ins_df = (
        pl.scan_parquet(ins_file_path)
        .with_columns(
            pl.when(pl.col("segment") == "INDICES")
            .then(False)
            .otherwise(True)
            .alias("eq_flag")
        )
        .select("symbol", "eq_flag")
    )

    query = f"""
            select *
            from {kite_conf["hist_table_name"]}
            """
    df = pl.read_database_uri(query=query, uri=db_conn)

    res = (
        df.lazy()
        .with_columns(pl.col("timestamp").cast(pl.Date()))
        .with_columns(
            # Close SMA expression
            [
                pl.col("close")
                .rolling_mean(window_size=n)
                .over(partition_by="symbol", order_by="timestamp", descending=False)
                .round(2)
                .alias(f"close_sma_{n}")
                for n in [50]
            ]
            # Close EMA Experssion
            + [
                pl.col("close")
                .ewm_mean(alpha=2 / (n + 1))
                .over(partition_by="symbol", order_by="timestamp", descending=False)
                .round(2)
                .alias(f"close_ema_{n}")
                for n in [9, 21]
            ]
            # Volume SMA Expression
            + [
                pl.col("volume")
                .rolling_mean(window_size=n)
                .over(partition_by="symbol", order_by="timestamp", descending=False)
                .round(0)
                .cast(pl.Int64())
                .alias(f"volume_sma_{n}")
                for n in [20]
            ]
            # Shift Columns
            + [
                pl.col(col)
                .shift(i)
                .over(partition_by="symbol", order_by="timestamp", descending=False)
                .alias(f"{col}_prev_{i}")
                for col in ["close", "timestamp"]
                for i in gains_dict
            ]
            # Day Range
            + [(pl.col("high") / pl.col("low")).alias("day_range")]
        )
        .with_columns(
            # Gains Calculation
            [
                (
                    (pl.col("close") - pl.col(f"close_prev_{i}"))
                    * 100
                    / pl.col(f"close_prev_{i}")
                )
                .round(4)
                .alias(f"pct_gain_prev_{i}")
                for i in gains_dict
            ]
            # ADR calculation
            + [
                (
                    (
                        pl.col("day_range")
                        .rolling_mean(window_size=20)
                        .over(
                            partition_by="symbol",
                            order_by="timestamp",
                            descending=False,
                        )
                        - 1
                    )
                    * 100
                )
                .round(2)
                .alias("adr_pct_20")
            ]
        )
        .with_columns(
            pl.when(pl.any_horizontal(pl.col("*").is_null()))
            .then(False)
            .otherwise(True)
            .alias("all_data_flag")
        )
        .join(ins_df, on="symbol", how="left")
    )

    return res


def basic_scan(data: pl.LazyFrame, gains_dict: dict = filters_dict) -> pl.LazyFrame:

    pct_gain_expr = reduce(
        lambda a, b: a | b,
        [
            pl.col(f"pct_gain_prev_{days}") >= threshold
            for days, threshold in gains_dict.items()
        ],
    )
    res = (
        data.filter((pl.col("eq_flag") == True) & (pl.col("all_data_flag") == True))
        .filter(
            (pl.col("close_ema_9") >= pl.col("close_sma_50"))
            & (pl.col("close_ema_21") >= pl.col("close_sma_50"))
            & (pl.col("volume_sma_20") >= VOLUME_THRESHOLD)
        )
        .filter(pct_gain_expr)
    )

    return res


def high_adr_scan(data: pl.LazyFrame, cut_off: float) -> pl.LazyFrame:

    res = basic_scan(data=data)
    res = res.filter(pl.col("adr_pct_20") >= cut_off)

    return res


def find_stocks(
    data: pl.LazyFrame, start_date: datetime, end_date: datetime
) -> pl.DataFrame:
    res = data.filter(
        pl.col("timestamp").is_between(
            lower_bound=start_date, upper_bound=end_date, closed="both"
        )
    ).collect()

    min_date = res.select(pl.col("timestamp").min()).item(0, 0)
    max_date = res.select(pl.col("timestamp").max()).item(0, 0)

    logger.info(f"MIN DATE IN DATA: {min_date} & PASEED DATE is {start_date}")

    res = (
        res.lazy()
        .select(pl.col("symbol").unique())
        .with_columns(pl.lit((max_date)).alias("scan_date"))
        .select("scan_date", "symbol")
        .collect()
    )

    return res
