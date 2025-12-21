from src.brokers.kite_broker import Kite
from src.config.market import Market
from src.config.exchange import Exchange
from src.config.brokers.kite import KiteConfig
from src.config.scans import scans_conf, filter_conf


RUN_MODES = {
    "1": {
        "broker": Kite,
        "market": Market.INDIA_EQUITIES,
        "exchange": Exchange.NSE,
        "config": KiteConfig,
        "scans_conf": scans_conf[Market.INDIA_EQUITIES],
        "filter_conf": filter_conf[Market.INDIA_EQUITIES],
    }
}
