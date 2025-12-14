import logging
from datetime import datetime

import polars as pl

from src.scans.conf import MID_DOWN_COUNT_THRESHOLD, VOLUME_THRESHOLD
from src.scans.swing_scan import add_basic_indicators

logger = logging.getLogger(__name__)


def basic_filter(
    data: pl.LazyFrame,
    symbol_list: list[str],
    scan_date: datetime,
    min_vol: float = VOLUME_THRESHOLD,
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
                & (pl.col("volume_sma_20") >= min_vol)
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
        .sort(["rvol_pct", "adr_pct_20"], descending=[False, True])
        .with_row_index(name="rank", offset=1)
        .collect()
    )


def pullback_filter(
    data: pl.LazyFrame,
    end_date: datetime,
    near_pct: float,
    adr_cutoff: float,
    down_count: int = MID_DOWN_COUNT_THRESHOLD,
) -> pl.DataFrame:
    comparisons = [
        (pl.col(f"mid_prev_{i}")) <= pl.col(f"mid_prev_{i + 1}") for i in range(1, 10)
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
                    <= near_pct
                ).alias(f"near_{col}")
                for col in ["close_ema_9", "close_ema_21", "close_sma_50"]
            ]
        )
        .with_columns(
            pl.sum_horizontal(
                [pl.when(cond).then(1).otherwise(0) for cond in comparisons]
            ).alias("mid_down_count")
        )
        .with_columns(mid_down_streak_expr)
        .filter(
            (
                (pl.col("near_close_ema_9") == True)
                | (pl.col("near_close_ema_21") == True)
                | (pl.col("near_close_sma_50") == True)
            )
            # & (pl.col("mid_down_count") > down_count)
            & (pl.col("timestamp") == end_date)
            & (pl.col("adr_pct_20") >= adr_cutoff)
            & (pl.col("rvol_pct") < 50)
        )
        .sort(["rvol_pct", "adr_pct_20"], descending=[False, True])
        .with_row_index(name="rank", offset=1)
    ).collect()

    return res
