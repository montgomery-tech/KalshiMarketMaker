from datetime import datetime, timezone
from typing import Dict, Optional, Tuple
import threading
import time

from ..factories import create_api, create_market_maker
from ..logging_utils import build_logger


def _is_unsupported_market(ticker: str, market_data: Dict) -> bool:
    """Reject MVE (multivariate event) and other unsupported market types.

    MVE markets are combo/parlay-style contracts that may not be sellable
    or behave differently from standard binary markets.
    """
    # Hard ticker-prefix gate: all MVE tickers start with KXMVE
    if ticker.upper().startswith("KXMVE"):
        return True

    # API response fields
    if market_data.get("mve_collection_ticker"):
        return True
    mve_legs = market_data.get("mve_selected_legs")
    if mve_legs is not None and len(mve_legs) > 0:
        return True

    # MVE combos use functional strikes
    strike_type = str(market_data.get("strike_type", "")).lower()
    if strike_type == "functional":
        return True

    # Non-binary markets
    market_type = str(market_data.get("market_type", "binary")).lower()
    if market_type != "binary":
        return True

    return False


def _parse_close_time(raw_close_time: str) -> Optional[int]:
    """Parse a Kalshi close_time string to a unix timestamp. Returns None on failure."""
    try:
        dt = datetime.fromisoformat(raw_close_time.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None


def _compute_T_and_close_ts(
    market_data: Dict,
    config_T: float,
    logger,
    ticker: str,
) -> Tuple[float, Optional[int]]:
    """Derive trading horizon T and close_time_ts from market data.

    Returns (T_seconds, close_time_ts_or_None).
    Falls back to config_T if close_time is missing or unparseable.
    """
    raw_close_time = market_data.get("close_time")
    if not raw_close_time:
        logger.warning(f"{ticker}: no close_time in market data, using config T={config_T:.0f}s")
        return config_T, None

    close_time_ts = _parse_close_time(raw_close_time)
    if close_time_ts is None:
        logger.warning(f"{ticker}: could not parse close_time '{raw_close_time}', using config T={config_T:.0f}s")
        return config_T, None

    time_until_close = close_time_ts - time.time()
    T = min(config_T, time_until_close)
    logger.info(f"{ticker}: market closes in {time_until_close:.0f}s, using T={T:.0f}s")
    return T, close_time_ts


def run_market_worker(
    ticker: str,
    dynamic_config: Dict,
    stop_event: threading.Event,
    shared_risk_state: Dict | None = None,
):
    logger = build_logger(f"Worker_{ticker}", dynamic_config.get("log_level", "INFO"))

    api = create_api(dynamic_config.get("api", {}), logger, market_ticker=ticker)

    # Safety gate: verify market is tradeable before committing
    try:
        market_response = api.get_market(ticker)
        market_data = market_response.get("market", {})
        if _is_unsupported_market(ticker, market_data):
            logger.error(
                f"BLOCKED: {ticker} is an unsupported market type "
                f"(market_type={market_data.get('market_type')}, "
                f"strike_type={market_data.get('strike_type')}, "
                f"mve_collection={market_data.get('mve_collection_ticker')}). "
                f"Refusing to trade."
            )
            api.logout()
            return
    except Exception as check_err:
        logger.error(f"Failed to verify market type for {ticker}, refusing to trade: {check_err}")
        api.logout()
        return

    mm_config = dynamic_config.get("market_maker", {})
    config_T = float(mm_config.get("T", 28800))
    T, close_time_ts = _compute_T_and_close_ts(market_data, config_T, logger, ticker)

    if T <= 60:
        logger.warning(f"{ticker}: market closes in {T:.0f}s, too soon to trade. Skipping.")
        api.logout()
        return

    market_maker = create_market_maker(
        mm_config,
        api,
        logger,
        dynamic_config.get("risk", {}),
        shared_risk_state,
        T_override=T,
        close_time_ts=close_time_ts,
    )
    dt = dynamic_config.get("dt", 2.0)

    try:
        logger.info(f"Starting market maker worker for {ticker}")
        market_maker.run(dt, stop_event=stop_event)
    except Exception as worker_exception:
        logger.error(f"Worker failed for {ticker}: {worker_exception}")
    finally:
        api.logout()
