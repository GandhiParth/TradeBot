from src.brokers.kite_broker import Kite
from src.brokers.polygon_broker import Polygon
from src.config.brokers.kite import KiteConfig
from src.config.brokers.polygon import PolygonConfig
from src.config.exchange import Exchange
from src.config.market import Market
from src.config.scans import filter_conf, scans_conf

RUN_MODES = {
    "1": {
        "broker": Kite,
        "market": Market.INDIA_EQUITIES,
        "exchange": Exchange.NSE,
        "config": KiteConfig,
        "scans_conf": scans_conf[Market.INDIA_EQUITIES],
        "filter_conf": filter_conf[Market.INDIA_EQUITIES],
    },
    "2": {
        "broker": Polygon,
        "market": Market.US_EQUITIES,
        "exchange": Exchange.NYSE,
        "config": PolygonConfig,
        "scans_conf": scans_conf[Market.US_EQUITIES],
        "filter_conf": filter_conf[Market.US_EQUITIES],
    },
    "3": {
        "broker": Polygon,
        "market": Market.US_EQUITIES,
        "exchange": Exchange.NASDAQ,
        "config": PolygonConfig,
        "scans_conf": scans_conf[Market.US_EQUITIES],
        "filter_conf": filter_conf[Market.US_EQUITIES],
    },
}
