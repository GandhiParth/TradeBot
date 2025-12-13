from conf import download_path, db_conn, kite as kite_conf
from src.scans.conf import MID_DOWN_COUNT_THRESHOLD, PULLBACK_NEAR_PCT
import polars as pl
from src.scans.swing_scan import basic_filter, add_basic_indicators
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


def pullback_filter(
    data: pl.LazyFrame,
    end_date: datetime,
    near_pct: float,
    adr_cutoff: float,
    down_count: int = MID_DOWN_COUNT_THRESHOLD,
):

    comparisons = [
        (pl.col(f"mid_prev_{i}")) <= pl.col(f"mid_prev_{i + 1}") for i in range(1, 10)
    ]
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
        .filter(
            (
                (pl.col("near_close_ema_9") == True)
                | (pl.col("near_close_ema_21") == True)
                | (pl.col("near_close_sma_50") == True)
            )
            & (pl.col("mid_down_count") > down_count)
            & (pl.col("timestamp") == end_date)
            & (pl.col("adr_pct_20") >= adr_cutoff)
        )
        .sort(["rvol_pct", "adr_pct_20"], descending=[False, True])
    ).collect()

    return res


if __name__ == "__main__":
    from src.utils import setup_logger
    import argparse

    parser = argparse.ArgumentParser(description="Run Filter Scans")
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--adr_cutoff", required=True, help="ADR Cutoff")
    args = parser.parse_args()

    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    adr_cutoff = float(args.adr_cutoff)

    setup_logger()

    scan_symbol_list = (
        pl.scan_parquet(download_path / "adr_stocks.parquet")
        .collect()
        .get_column("symbol")
        .to_list()
    )

    logger.info(f"Stocks in the Scan List {len(scan_symbol_list)}")

    query = f"""
    select * 
    from {kite_conf["hist_table_name"]}
    where symbol in {tuple(scan_symbol_list)}
    """
    data = pl.read_database_uri(query=query, uri=db_conn)

    basic_stock_list = basic_filter(
        data=data, symbol_list=scan_symbol_list, scan_date=end_date
    )
    query = f"""
    select * 
    from {kite_conf["hist_table_name"]}
    where symbol in {tuple(basic_stock_list)}
    """
    data = pl.read_database_uri(query=query, uri=db_conn)

    pullback_df = pullback_filter(
        data=data, end_date=end_date, near_pct=PULLBACK_NEAR_PCT, adr_cutoff=adr_cutoff
    )
    pullback_df.write_parquet(download_path + f"/pullback.parquet")
