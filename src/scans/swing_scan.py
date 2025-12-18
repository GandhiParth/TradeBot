import logging
from datetime import datetime
from functools import reduce

import polars as pl

logger = logging.getLogger(__name__)


def add_basic_indicators(data: pl.LazyFrame) -> pl.LazyFrame:
    """
    Add SMA, EMA, ADR & RVOL Columns
    """
    res = (
        data.lazy()
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
                for n in [20, 50]
            ]
            # Day Range
            + [(pl.col("high") / pl.col("low")).round(4).alias("day_range")]
            # Body to Range ratio
            + [
                (
                    (
                        ((pl.col("open") - pl.col("close")).abs())
                        / (pl.col("high") - pl.col("low"))
                    ).rolling_mean(window_size=n)
                    * 100
                )
                .over(partition_by="symbol", order_by="timestamp", descending=False)
                .round(2)
                .alias(f"body_by_range_pct_sma_{n}")
                for n in [50]
            ]
            # Lower Wick to Body Ratio
            + [
                (
                    (
                        ((pl.min_horizontal("open", "close") - pl.col("low")))
                        / (pl.col("high") - pl.col("low"))
                    ).rolling_mean(window_size=n)
                    * 100
                )
                .over(partition_by="symbol", order_by="timestamp", descending=False)
                .round(2)
                .alias(f"lower_wick_by_range_pct_sma_{n}")
                for n in [50]
            ]
            # Uppwer Wick to Body Ratio
            + [
                (
                    (
                        ((pl.col("high") - pl.max_horizontal("open", "close")))
                        / (pl.col("high") - pl.col("low"))
                    ).rolling_mean(window_size=n)
                    * 100
                )
                .over(partition_by="symbol", order_by="timestamp", descending=False)
                .round(2)
                .alias(f"upper_wick_by_range_pct_sma_{n}")
                for n in [50]
            ]
        )
        .with_columns(
            # ADR calculation
            [
                (
                    (
                        pl.col("day_range")
                        .rolling_mean(window_size=i)
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
                .alias(f"adr_pct_{i}")
                for i in [20]
            ]
            # RVOL calculation
            + [
                (pl.col("volume") * 100 / pl.col("volume_sma_50"))
                .round()
                .alias("rvol_pct")
            ]
            # Clean Score Calculation
            + [
                (
                    pl.col(f"body_by_range_pct_sma_{n}")
                    * (100 - pl.col(f"lower_wick_by_range_pct_sma_{n}"))
                )
                .round(2)
                .alias(f"clean_score_pct_{n}")
                for n in [50]
            ]
        )
    )

    return res


def prep_scan_data(
    ins_file_path: str,
    conn: str,
    kite_conf: dict,
    lookback_min_gains_dict: dict,
) -> pl.LazyFrame:
    """
    Fetch all data from DB and prepare it for scan
    """
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
            from {kite_conf["hist_table_id"]}
            """
    df = pl.read_database_uri(query=query, uri=conn)
    df = add_basic_indicators(data=df)

    res = (
        df.with_columns(
            # Shift Columns
            [
                pl.col(col)
                .shift(i)
                .over(partition_by="symbol", order_by="timestamp", descending=False)
                .alias(f"{col}_prev_{i}")
                for col in ["close", "timestamp"]
                for i in lookback_min_gains_dict
            ]
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
                for i in lookback_min_gains_dict
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


def basic_scan(data: pl.LazyFrame, conf: dict) -> pl.LazyFrame:
    """
    Basic Scan checking if EMA's and Vol are aligned along with Past Pct Gains
    """
    pct_gain_expr = reduce(
        lambda a, b: a | b,
        [
            pl.col(f"pct_gain_prev_{days}") >= threshold
            for days, threshold in conf["lookback_min_return_pct"].items()
        ],
    )
    res = data.filter(
        (pl.col("eq_flag") == True)
        & (pl.col("all_data_flag") == True)
        & (pl.col("close_ema_9") >= pl.col("close_sma_50"))
        & (pl.col("close_ema_21") >= pl.col("close_sma_50"))
        & (pl.col("volume_sma_20") >= conf["volume_threshold"])
    ).filter(pct_gain_expr)

    return res


def high_adr_scan(data: pl.LazyFrame, adr_cutoff: float, conf: dict) -> pl.LazyFrame:
    """
    High ADR cutoff on top of Basic Scan
    """
    res = basic_scan(data=data, conf=conf)
    res = res.filter(pl.col("adr_pct_20") >= adr_cutoff)

    return res


def find_stocks(
    data: pl.LazyFrame, start_date: datetime, end_date: datetime
) -> pl.DataFrame:
    """
    Get the unique stocks flagged between the date ranges
    """
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
