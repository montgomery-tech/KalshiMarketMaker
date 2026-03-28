import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional

from dotenv import find_dotenv, load_dotenv

from ..factories import create_api
from ..logging_utils import build_logger


def safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def format_price(cents) -> str:
    val = safe_int(cents)
    if val == 0:
        return " -- "
    return f"{val:3d}¢"


def parse_close_time(market: Dict) -> Optional[datetime]:
    raw = market.get("close_time") or market.get("expiration_time")
    if not raw:
        return None
    try:
        ts = raw.rstrip("Z")
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return None


def is_live(market: Dict, live_hours: float) -> bool:
    close_dt = parse_close_time(market)
    if close_dt is None:
        return False
    now = datetime.now(tz=timezone.utc)
    delta_hours = (close_dt - now).total_seconds() / 3600
    return 0 < delta_hours <= live_hours


def print_sports_markets(markets: List[Dict], series_filter: str = None) -> None:
    if series_filter:
        markets = [m for m in markets if m.get("series_ticker", "").upper() == series_filter.upper()]

    if not markets:
        print("No live sports contracts found.")
        return

    header = (
        f"{'TICKER':<36}  {'YES BID':>7}  {'YES ASK':>7}  {'VOL 24H':>9}  {'OPEN INT':>9}  {'CLOSES':>8}  TITLE"
    )
    separator = "-" * min(140, len(header) + 10)
    print(separator)
    print(header)
    print(separator)

    now = datetime.now(tz=timezone.utc)
    for market in markets:
        ticker = market.get("ticker", "UNKNOWN")
        yes_bid = format_price(market.get("yes_bid"))
        yes_ask = format_price(market.get("yes_ask"))
        volume_24h = safe_int(market.get("volume_24h", 0))
        open_interest = safe_int(market.get("open_interest", 0))
        title = market.get("title", "")
        subtitle = market.get("subtitle", "")
        display_title = f"{title}: {subtitle}" if subtitle else title

        close_dt = parse_close_time(market)
        if close_dt:
            mins_left = int((close_dt - now).total_seconds() / 60)
            closes_str = f"{mins_left}m" if mins_left < 120 else f"{mins_left // 60}h{mins_left % 60:02d}m"
        else:
            closes_str = "  --  "

        print(
            f"{ticker:<36}  {yes_bid:>7}  {yes_ask:>7}  {volume_24h:>9,}  {open_interest:>9,}  {closes_str:>8}  {display_title}"
        )

    print(separator)
    print(f"Total: {len(markets)} contract(s)")


def main():
    parser = argparse.ArgumentParser(description="List currently live Kalshi sports contracts")
    parser.add_argument(
        "--live-hours",
        type=float,
        default=6.0,
        help="Show markets closing within this many hours (default: 6). Use --all to skip this filter.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Show all open sports markets, not just live ones",
    )
    parser.add_argument(
        "--series",
        type=str,
        default=None,
        help="Filter by series ticker (e.g. NBA, NFL, SOCCER)",
    )
    parser.add_argument(
        "--sort",
        type=str,
        choices=["ticker", "volume", "open_interest", "closes"],
        default="closes",
        help="Sort results by field (default: closes)",
    )
    parser.add_argument(
        "--min-volume",
        type=int,
        default=0,
        help="Minimum 24h volume filter",
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=250,
        help="Markets per page (default: 250)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Maximum pages to fetch (default: 10)",
    )
    parser.add_argument("--log-level", type=str, default="WARNING", help="Logging level")
    args = parser.parse_args()

    load_dotenv(find_dotenv(usecwd=True))
    logger = build_logger("ListSports", args.log_level)
    api = create_api({}, logger, market_ticker="DYNAMIC")

    try:
        markets = api.list_all_open_markets(
            category="Sports",
            page_limit=args.page_limit,
            max_pages=args.max_pages,
            max_markets=args.page_limit * args.max_pages,
        )

        if not args.all:
            markets = [m for m in markets if is_live(m, args.live_hours)]

        if args.min_volume > 0:
            markets = [m for m in markets if safe_int(m.get("volume_24h", 0)) >= args.min_volume]

        now = datetime.now(tz=timezone.utc)
        if args.sort == "ticker":
            markets.sort(key=lambda m: m.get("ticker", ""))
        elif args.sort == "open_interest":
            markets.sort(key=lambda m: safe_int(m.get("open_interest", 0)), reverse=True)
        elif args.sort == "volume":
            markets.sort(key=lambda m: safe_int(m.get("volume_24h", 0)), reverse=True)
        else:
            markets.sort(key=lambda m: (parse_close_time(m) or datetime.max.replace(tzinfo=timezone.utc)))

        print_sports_markets(markets, series_filter=args.series)
    finally:
        api.logout()


if __name__ == "__main__":
    main()
