import logging
from typing import Literal

import pandas as pd
import polars as pl
from kiteconnect import KiteConnect

logger = logging.getLogger(__name__)


def fetch_instruments(
    kite: KiteConnect,
    exchange: Literal[
        "BFO",
        "BSE",
        "CDS",
        "GLOBAL",
        "MCX",
        "NCO",
        "NFO",
        "NSE",
        "NSEIX",
    ],
) -> pl.DataFrame:
    """
    Downloads the instrument list for different exchanges as CSV file
    at the download path given

    Parameters:
    kite (KiteConnect): KiteConnect object ot fetch the instrument list
    download_path (str): The path to download the instruments list
    exchanges List[str]: List of exchnages to download for.
    """

    ins_schema = {
        "instrument_token": pl.String,
        "exchange_token": pl.String,
        "tradingsymbol": pl.String,
        "name": pl.String,
        "last_price": pl.Float64,
        "expiry": pl.String,
        "strike": pl.Float64,
        "tick_size": pl.Float64,
        "lot_size": pl.Int64,
        "instrument_type": pl.String,
        "segment": pl.String,
        "exchange": pl.String,
    }

    try:
        ins = kite.instruments(exchange=exchange)
        df = pd.DataFrame(ins)
        df = df.astype(
            {col: "string" for col in df.select_dtypes(include="object").columns}
        )

        df = pl.from_pandas(df, schema_overrides=ins_schema).rename(
            {"tradingsymbol": "symbol"}
        )

        logger.info(f"""Successfully Fetched Instruments for {exchange}""")

        return df

    except Exception as e:
        logger.warning(f"""Failed to fetch instrument list for {exchange}.""")
        logger.error(e)
