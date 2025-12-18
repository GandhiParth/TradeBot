import logging
from datetime import datetime

import polars as pl

from src.scans.swing_scan import add_basic_indicators

logger = logging.getLogger(__name__)


def basic_filter(
    data: pl.LazyFrame,
    symbol_list: list[str],
    scan_date: datetime,
    conf: dict,
) -> list[str]:
    logger.info(f"Number of stocks in symbol list: {len(symbol_list)}")

    df = add_basic_indicators(data=data)

    res = (
        (
            df.lazy()
            .with_columns(
                pl.when(pl.any_horizontal(pl.col("*").is_null()))
                .then(False)
                .otherwise(True)
                .alias("all_data_flag")
            )
            .filter(
                (pl.col("close_ema_9") >= pl.col("close_sma_50"))
                & (pl.col("close_ema_21") >= pl.col("close_sma_50"))
                & (pl.col("volume_sma_20") >= conf["volume_threshold"])
                & (pl.col("timestamp") == scan_date)
                & (pl.col("symbol").is_in(symbol_list))
            )
            .select("symbol")
            .unique()
            .sort("symbol")
        )
        .collect()
        .get_column("symbol")
        .to_list()
    )

    logger.info(f"Symbols after basic filter: {len(res)}")

    return res


def adr_filter(
    data: pl.LazyFrame | pl.DataFrame, adr_cutoff: float, end_date: datetime
):
    df = add_basic_indicators(data=data)

    return (
        df.lazy()
        .filter(
            (pl.col("timestamp") == end_date) & (pl.col("adr_pct_20") >= adr_cutoff)
        )
        .sort(["adr_pct_20", "rvol_pct"], descending=[True, False])
        .with_row_index(name="rank", offset=1)
        .collect()
    )


def pullback_filter(
    data: pl.LazyFrame,
    adr_cutoff: float,
    end_date: datetime,
    conf: dict,
) -> pl.DataFrame:
    comparisons = [
        (pl.col(f"mid_prev_{i}")) <= pl.col(f"mid_prev_{i + 1}") for i in range(0, 10)
    ]

    cumulative_conditions = []
    current_chain = pl.lit(True)

    for cond in comparisons:
        # "Current streak is alive IF it was alive before AND this condition is met"
        current_chain = current_chain & cond
        cumulative_conditions.append(current_chain)

    # 3. Sum the cumulative conditions to get the streak count
    mid_down_streak_expr = pl.sum_horizontal(cumulative_conditions).alias(
        "mid_down_streak"
    )

    df = add_basic_indicators(data=data)
    res = (
        df.lazy()
        .with_columns(
            [pl.mean_horizontal(("open", "close")).round(2).alias("mid_prev_0")]
        )
        .with_columns(
            [
                pl.col("mid_prev_0")
                .shift(i)
                .over(partition_by="symbol", order_by="timestamp", descending=False)
                .alias(f"mid_prev_{i}")
                for i in range(1, 11)
            ]
            + [
                (
                    ((pl.col("mid_prev_0") - pl.col(col)).abs() * 100 / pl.col(col))
                    <= conf["pullback_near_pct"]
                ).alias(f"near_{col}")
                for col in ["close_ema_9", "close_ema_21", "close_sma_50"]
            ]
        )
        .with_columns(mid_down_streak_expr)
        .filter(
            (
                (pl.col("near_close_ema_9") == True)
                | (pl.col("near_close_ema_21") == True)
                | (pl.col("near_close_sma_50") == True)
            )
            & (pl.col("timestamp") == end_date)
            & (pl.col("adr_pct_20") >= adr_cutoff)
            & (pl.col("rvol_pct") < 100)
        )
        .sort(["adr_pct_20", "rvol_pct"], descending=[True, False])
        .with_row_index(name="rank", offset=1)
    ).collect()

    return res


def vcp_filter(data: pl.DataFrame, end_date: datetime, conf: dict) -> pl.DataFrame:
    """ """

    df = add_basic_indicators(data=data)

    timeframe = conf["timeframe"]
    volume_timeframe = conf["volume_timeframe"]
    pivot_length = conf["pivot_length"]
    pivot_width_limit_pct = conf["pivot_width_limit_pct"]
    base_lower_limit_pct = conf["base_lower_limit_pct"]

    res = (
        df.lazy()
        .with_columns(
            # 52 week high calculation
            pl.col("close")
            .rolling_max(window_size=timeframe)
            .over(partition_by="symbol", order_by="timestamp", descending=False)
            .alias("52_week_high"),
            # volume sma calculation
            pl.col("volume")
            .rolling_mean(window_size=volume_timeframe)
            .over(partition_by="symbol", order_by="timestamp", descending=False)
            .alias(f"volume_sma_{volume_timeframe}"),
            # pivot high calculation
            pl.col("high")
            .rolling_max(window_size=pivot_length)
            .over(partition_by="symbol", order_by="timestamp", descending=False)
            .alias("pivot_high"),
            # pivot low calculation
            pl.col("low")
            .rolling_min(window_size=pivot_length)
            .over(partition_by="symbol", order_by="timestamp", descending=False)
            .alias("pivot_low"),
            # pivot start high
            pl.col("high")
            .shift(pivot_length - 1)
            .over(partition_by="symbol", order_by="timestamp", descending=False)
            .alias("pivot_start_high"),
        )
        .with_columns(
            # pivot width
            ((pl.col("pivot_high") - pl.col("pivot_low")) * 100 / pl.col("close"))
            .round(4)
            .alias("pivot_width")
        )
        .with_columns(
            # find pivot
            (
                (pl.col("pivot_width") < pivot_width_limit_pct)
                & (pl.col("pivot_high") == pl.col("pivot_start_high"))
            ).alias("is_pivot"),
            # volume dry up
            pl.all_horizontal(
                [
                    pl.col("volume").shift(i)
                    < pl.col(f"volume_sma_{volume_timeframe}").shift(i)
                    for i in range(pivot_length)
                ]
            )
            .over(partition_by="symbol", order_by="timestamp", descending=False)
            .alias("vol_dry_up"),
            # near 52 week high
            (
                (pl.col("close") < pl.col("52_week_high"))
                & (
                    pl.col("close")
                    > ((base_lower_limit_pct / 100) * pl.col("52_week_high"))
                )
            ).alias("near_high"),
        )
        .filter(
            pl.col("near_high")
            & pl.col("is_pivot")
            & pl.col("vol_dry_up")
            & (pl.col("timestamp") == end_date)
        )
        .sort(["adr_pct_20", "rvol_pct"], descending=[True, False])
        .with_row_index(name="rank", offset=1)
        .collect()
    )

    return res
