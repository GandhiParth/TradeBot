import argparse
import logging

import polars as pl
import polars.selectors as cs

from src.config.brokers.nse import NSEConfig
from src.config.data_source import DataSource
from src.config.exchange import Exchange
from src.config.market import Market
from src.config.storage_layout import StorageLayout
from src.data_source.chartsmaze.helper import industry_to_sector
from src.data_source.chartsmaze.sectors import sectors as cmaze_sectors
from src.utils import setup_logger

logger = logging.getLogger(__name__)
setup_logger()


def _make_dir(end_date: str, market: Market, exchange: Exchange):
    analysis_path = StorageLayout.analysis_dir(
        run_date=end_date, market=market, exchange=exchange
    )
    analysis_path.mkdir(parents=True, exist_ok=True)
    logger.info(f"Creating Directory: {analysis_path}")

    return analysis_path


def _fetch_cmaze_file(end_date: str, sectors_mapping: dict) -> pl.LazyFrame:
    cmaze_path = StorageLayout.data_dir(market=Market.INDIA, exchange=DataSource.CMAZE)
    cmaze_sectors_df = industry_to_sector(mapping=sectors_mapping).lazy()

    cmaze_df = (
        pl.scan_csv(cmaze_path / f"{end_date}.csv")
        .join(cmaze_sectors_df, on="Basic Industry", how="left")
        .group_by(pl.exclude("Sector"))
        .agg(pl.col("Sector").str.join(", "))
        .rename(
            {
                "Stock Name": "symbol",
                "RS Rating": "rs_rating",
                "Basic Industry": "basic_industry_cmaze",
                "Sector": "sector_cmaze",
                "Market Cap(Cr.)": "market_cap_cr_cmaze",
                "1 Month Returns(%)": "1_mo_rtr_pct",
                "3 Month Returns(%)": "3_mo_rtr_pct",
                "% from 52W High": "pct_from_52w_high",
            }
        )
        .select(
            [
                "symbol",
                "rs_rating",
                "basic_industry_cmaze",
                "sector_cmaze",
                "market_cap_cr_cmaze",
                "1_mo_rtr_pct",
                "3_mo_rtr_pct",
                "pct_from_52w_high",
            ]
        )
    )

    return cmaze_df


def _fetch_nse_sectors() -> pl.LazyFrame:
    db_path = StorageLayout.db_path(market=Market.INDIA, exchange=Exchange.NSE)

    max_date_query = f"""
    select max(timestamp) as timestamp
    from '{NSEConfig.CLASSIFICATION_TABLE_ID}'
    """

    max_date = pl.read_database_uri(
        query=max_date_query, uri=f"sqlite:///{db_path}"
    ).item(0, 0)

    logger.info(f"MAX DATE of NSE SECTORS: {max_date}")

    industry_query = f"""
    select distinct *
    from '{NSEConfig.CLASSIFICATION_TABLE_ID}'
    where timestamp = '{max_date}'
    """

    nse_classify_df = (
        pl.read_database_uri(query=industry_query, uri=f"sqlite:///{db_path}")
        .lazy()
        .rename({"timestamp": "latest_fetch_date"})
    )

    return nse_classify_df


def _combine_filers_files(end_date: str) -> pl.LazyFrame:
    filters_path = StorageLayout.filters_dir(
        run_date=end_date, market=Market.INDIA_EQUITIES, exchange=Exchange.NSE
    )

    basic_filter = (
        pl.scan_csv(filters_path / "basic_filter.csv")
        .with_columns(
            pl.col("timestamp")
            .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f")
            .cast(pl.Date)
            .alias("timestamp")
        )
        .select("timestamp", "symbol")
    )

    df_list = []
    for filter_type in [
        "sma_200",
        "adr",
        "pullback",
    ]:
        df = (
            pl.scan_csv(filters_path / f"{filter_type}_filter.csv")
            .with_columns(pl.lit(True).alias(f"{filter_type}_filter_flag"))
            .select("symbol", f"{filter_type}_filter_flag")
        )
        df_list.append(df)

    res = (
        basic_filter.join(df_list[0], on="symbol", how="left")
        .join(df_list[1], on="symbol", how="left")
        .join(df_list[2], on="symbol", how="left")
        .with_columns(cs.ends_with("flag").fill_null(False))
        .collect()
    )

    logger.info(f"Overall Before RS filter: {res.shape}")
    return res.lazy()


def _pullback_filters_file(end_date: str) -> pl.LazyFrame:
    filters_path = StorageLayout.filters_dir(
        run_date=end_date, market=Market.INDIA_EQUITIES, exchange=Exchange.NSE
    )

    basic_filter = (
        pl.scan_csv(filters_path / "basic_filter.csv")
        .with_columns(
            pl.col("timestamp")
            .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f")
            .cast(pl.Date)
            .alias("timestamp")
        )
        .select("timestamp", "symbol")
    )

    df_list = []
    for filter_type in ["sma_200", "adr"]:
        df = (
            pl.scan_csv(filters_path / f"{filter_type}_filter.csv")
            .with_columns(pl.lit(True).alias(f"{filter_type}_filter_flag"))
            .select("symbol", f"{filter_type}_filter_flag")
        )
        df_list.append(df)

    for filter_type in ["pullback"]:
        df = (
            pl.scan_csv(filters_path / f"{filter_type}_filter.csv")
            .with_columns(pl.lit(True).alias(f"{filter_type}_filter_flag"))
            .select(
                "symbol",
                f"{filter_type}_filter_flag",
                "near_ema_9",
                "near_ema_21",
                "near_sma_50",
                "mid_down_streak",
            )
        )
        df_list.append(df)

    res = (
        basic_filter.join(df_list[0], on="symbol", how="left")
        .join(df_list[1], on="symbol", how="left")
        .join(df_list[2], on="symbol", how="left")
        .with_columns(cs.ends_with("flag").fill_null(False))
        .collect()
    )

    logger.info(f"Pullback Before RS filter: {res.shape}")
    return res.lazy()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Filter Scans Analysis")
    parser.add_argument("--end_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--rs_cutoff", default=70, help="RS Cutoff")
    args = parser.parse_args()

    end_date = args.end_date
    rs_cutoff = int(args.rs_cutoff)

    analysis_path = _make_dir(
        end_date=end_date, market=Market.INDIA_EQUITIES, exchange=Exchange.NSE
    )
    cmaze_df = _fetch_cmaze_file(end_date=end_date, sectors_mapping=cmaze_sectors)
    nse_classify_df = _fetch_nse_sectors()
    res = _combine_filers_files(end_date=end_date)

    res = (
        res.join(cmaze_df, on="symbol", how="left")
        .join(nse_classify_df, on="symbol", how="left")
        .collect()
    )

    res.write_csv(analysis_path / "overall_filter_result.csv")

    res_cutoff = res.filter(pl.col("rs_rating") >= rs_cutoff)
    logger.info(f"Overall After RS filter: {res_cutoff.shape}")

    res = _pullback_filters_file(end_date=end_date)

    res = (
        res.join(cmaze_df, on="symbol", how="left")
        .join(nse_classify_df, on="symbol", how="left")
        .collect()
    )

    res.write_csv(analysis_path / "pullback_filter_result.csv")

    res_cutoff = res.filter(pl.col("rs_rating") >= rs_cutoff)
    logger.info(f"Pullback After RS filter: {res_cutoff.shape}")
