from src.config.market import Market

_VOLUME_THRESHOLD = 10_00_00

_INDIA_LOOKBACK_DAYS_TO_MIN_RETURN_PCT = {
    1: 4.99,  # close today ≥ close 1 day ago * 1.05
    22: 10,  # close today ≥ close 22 days ago * 1.10
    67: 20,  # close today ≥ close 67 days ago * 1.22
    125: 60,  # close today ≥ close 125 days ago * 1.60
}

_US_LOOKBACK_DAYS_TO_MIN_RETURN_PCT = {
    1: 10,  # close today ≥ close 1 day ago * 1.10
    22: 25,  # close today ≥ close 22 days ago * 1.25
    67: 50,  # close today ≥ close 67 days ago * 1.50
    125: 100,  # close today ≥ close 125 days ago * 2
}

_MID_DOWN_COUNT_THRESHOLD = 2
_PULLBACK_NEAR_PCT = 2  # in %
_PULLBACK_DAYS = 10

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
        "lookback_min_return_pct": _INDIA_LOOKBACK_DAYS_TO_MIN_RETURN_PCT,
    },
    Market.US_EQUITIES: {
        "months_lookback": 3,
        "data_lookback_days": 500,
        "volume_threshold": _VOLUME_THRESHOLD,
        "lookback_min_return_pct": _US_LOOKBACK_DAYS_TO_MIN_RETURN_PCT,
    },
}

filter_conf = {
    Market.INDIA_EQUITIES: {
        "pullback": {
            "pullback_near_pct": _PULLBACK_NEAR_PCT,
            "mid_down_strak": _MID_DOWN_COUNT_THRESHOLD,
            "pullback_days": _PULLBACK_DAYS,
        },
        "vcp": {**_VCP_FILTER_CONF},
    },
    Market.US_EQUITIES: {
        "pullback": {
            "pullback_near_pct": _PULLBACK_NEAR_PCT,
            "mid_down_strak": _MID_DOWN_COUNT_THRESHOLD,
        },
        "vcp": {**_VCP_FILTER_CONF},
    },
}
