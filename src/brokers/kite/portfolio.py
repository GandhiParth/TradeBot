import logging

import polars as pl
from kiteconnect import KiteConnect

from src.config.brokers.kite import KitePortfolioConfig

logger = logging.getLogger(__name__)


def get_portfolio(
    kite: KiteConnect, conf: KitePortfolioConfig, blacklist_flag: bool = True
) -> pl.DataFrame:
    """
    Generates the portfolio holdings
    """

    portfoltio = (
        pl.DataFrame(kite.holdings())
        .select(
            [
                "tradingsymbol",
                "quantity",
                "t1_quantity",
                "average_price",
                "last_price",
                "pnl",
            ]
        )
        .filter((pl.col("quantity") > 0) | (pl.col("t1_quantity") > 0))
        .with_columns((pl.col("quantity") + pl.col("t1_quantity")).alias("quantity"))
        .select(pl.exclude("t1_quantity"))
    )

    positions = pl.DataFrame(kite.positions(), strict=False).select(pl.col("net"))

    if positions.shape[0] > 0:
        positions = (
            positions.select(pl.col("net").struct.unnest())
            .filter(pl.col("day_buy_quantity") > 0)
            .select(
                [
                    "tradingsymbol",
                    "quantity",
                    "average_price",
                    "last_price",
                    "pnl",
                ]
            )
        )

        tmp = pl.concat([portfoltio, positions], how="vertical_relaxed")
    else:
        tmp = portfoltio

    margin = (
        pl.DataFrame(kite.margins())
        .select(pl.col("equity").struct.unnest())
        .select("net")
        .item(0, 0)
    )

    logger.info("Portoflio, Positions & Margins Fetched Successfully")

    margins_df = pl.DataFrame(
        {
            "tradingsymbol": "CASH",
            "quantity": 1,
            "average_price": margin,
            "last_price": margin,
            "pnl": 0,
            "invest_amt": margin,
            "ptf_value": margin,
        }
    )

    tmp = (
        tmp.filter(pl.col("quantity") > 0)
        .with_columns(
            (pl.col("quantity") * pl.col("average_price")).round(2).alias("invest_amt")
        )
        .group_by("tradingsymbol")
        .agg(
            pl.col("quantity").sum(),
            (pl.col("invest_amt").sum() / pl.col("quantity").sum())
            .round(2)
            .alias("average_price"),
            pl.col("last_price").first(),
            pl.col("pnl").sum().round(2),
        )
        .with_columns(
            (pl.col("quantity") * pl.col("average_price")).round(2).alias("invest_amt"),
        )
        .with_columns(
            (pl.col("invest_amt") + pl.col("pnl")).round(2).alias("ptf_value")
        )
    )

    res = pl.concat([tmp, margins_df], how="vertical_relaxed")

    if blacklist_flag:
        res = res.filter(~pl.col("tradingsymbol").is_in(conf.blacklist_symbol))

    final = (
        res.with_columns(
            ((pl.col("last_price") / pl.col("average_price") - 1) * 100)
            .round(2)
            .alias("pnl_pct"),
            (pl.col("invest_amt") * 100 / pl.col("invest_amt").sum())
            .round(2)
            .alias("invest_pct"),
            (pl.col("ptf_value") * 100 / pl.col("ptf_value").sum())
            .round(2)
            .alias("ptf_pct"),
        )
        .sort(["pnl_pct"], descending=[True])
        .rename(
            {
                "tradingsymbol": "Symbol",
                "quantity": "Quantity",
                "average_price": "Avg_Price",
                "last_price": "LTP",
                "pnl": "PnL",
                "invest_amt": "Invested_Amt",
                "ptf_value": "Portolio_Value",
                "pnl_pct": "PnL_Pct",
                "invest_pct": "Invested_Pct",
                "ptf_pct": "Portfolio_Pct",
            }
        )
    )

    return final


class Ptf_Attr:
    """
    Gives Different Attributes of the Portfolio
    """

    def __init__(
        self,
        ptf: pl.DataFrame,
        conf: KitePortfolioConfig,
    ):
        self._ptf = ptf
        self._risk_pct = conf.RISK_PCT
        self._pst_size_pct = conf.POSITION_SIZE_PORTFOLIO_PCT
        self._max_sl_pct = conf.MAX_SL_RISK_PCT

    @property
    def invested_amt(self) -> float:
        return round(self._ptf.select("Invested_Amt").sum().item(0, 0), 2)

    @property
    def portfolio_value(self) -> float:
        return round(self._ptf.select("Portolio_Value").sum().item(0, 0), 2)

    def risk_amt_per_trade(self, risk_pct: float = None) -> float:
        if risk_pct is None:
            risk_pct = self._risk_pct

        return round(
            self._ptf.select("Invested_Amt").sum().item(0, 0) * (risk_pct / 100)
        )

    @property
    def ptf(self) -> pl.DataFrame:
        return self._ptf

    def position_size(self, sl_pct: float, risk_pct: float = None) -> float:
        if sl_pct > self._max_sl_pct:
            return 0
        if risk_pct is None:
            risk_pct = self._risk_pct

        if risk_pct > self._risk_pct:
            return 0

        size_sl = round(self.risk_amt_per_trade(risk_pct) * 100 / sl_pct)
        size_pos = round(self.invested_amt * self._pst_size_pct / 100)
        return min(size_sl, size_pos)

    def overall_return(self, initial_amt: float) -> float:
        return round((self.portfolio_value - initial_amt) * 100 / initial_amt, 2)
