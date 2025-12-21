from src.config.market import Market

_VOLUME_THRESHOLD = 10_00_00

_LOOKBACK_DAYS_TO_MIN_RETURN_PCT = {
    1: 4.99,  # close today ≥ close 1 day ago * 1.05
    22: 10,  # close today ≥ close 22 days ago * 1.10
    67: 20,  # close today ≥ close 67 days ago * 1.22
    125: 60,  # close today ≥ close 125 days ago * 1.60
}

_MID_DOWN_COUNT_THRESHOLD = 2
_PULLBACK_NEAR_PCT = 2  # in %

_VCP_FILTER_CONF = {
    "timeframe": 252,
    "volume_timeframe": 50,
    "base_lower_limit_pct": 60,  # in % eg: 60%
    "pivot_length": 5,
    "pivot_width_limit_pct": 10,  # in % eg: 10%
}

scans_conf = {
    Market.INDIA_EQUITIES: {
        "months_lookback": 3,
        "data_lookback_days": 500,
        "volume_threshold": _VOLUME_THRESHOLD,
        "lookback_min_return_pct": _LOOKBACK_DAYS_TO_MIN_RETURN_PCT,
    }
}

filter_conf = {
    Market.INDIA_EQUITIES: {
        "pullback": {
            "pullback_near_pct": _PULLBACK_NEAR_PCT,
            "mid_down_strak": _MID_DOWN_COUNT_THRESHOLD,
        },
        "vcp": {**_VCP_FILTER_CONF},
    }
}
