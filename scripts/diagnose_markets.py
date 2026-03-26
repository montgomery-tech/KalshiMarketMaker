#!/usr/bin/env python3
"""
Diagnose why the market selector is finding zero tradeable markets.

Usage:
    python3 scripts/diagnose_markets.py [--limit N] [--category CAT] [--raw]

Reads credentials from the same env vars as the main bot:
    KALSHI_BASE_URL, KALSHI_API_KEY_ID, KALSHI_PRIVATE_KEY_PATH
"""

import argparse
import statistics
import sys
import os

# Allow running from repo root without installing the package
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from kalshi_market_maker.logging_utils import build_logger
from kalshi_market_maker.factories import create_api
from kalshi_market_maker.selection.scoring import (
    compute_spread_cents,
    is_supported_binary_market,
    safe_float,
)

DEFAULT_LIMIT = 50
DEFAULT_CATEGORY = "sports"
MIN_VOLUME_24H = 500
MIN_SPREAD_CENTS = 2


def parse_args():
    p = argparse.ArgumentParser(description="Diagnose Kalshi market selector filters")
    p.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max markets to fetch")
    p.add_argument("--category", type=str, default=DEFAULT_CATEGORY, help="Kalshi category filter")
    p.add_argument("--no-category", action="store_true", help="Fetch without any category filter")
    p.add_argument("--raw", action="store_true", help="Print raw fields of first market object")
    p.add_argument("--min-volume", type=float, default=MIN_VOLUME_24H, help="Volume threshold (default 500)")
    p.add_argument("--min-spread", type=float, default=MIN_SPREAD_CENTS, help="Spread threshold cents (default 2)")
    return p.parse_args()


def main():
    args = parse_args()
    logger = build_logger("Diagnose")

    api = create_api({}, logger, market_ticker="DIAGNOSTIC")

    category = None if args.no_category else args.category
    label = f"category={category!r}" if category else "no category filter"
    print(f"\nFetching markets ({label}, limit={args.limit})...\n")

    response = api.list_markets(
        status="open",
        limit=args.limit,
        mve_filter="exclude",
        category=category,
    )
    markets = response.get("markets", [])
    print(f"Total fetched: {len(markets)}\n")

    if not markets:
        print("No markets returned — check credentials and KALSHI_BASE_URL.")
        return

    # --raw: dump all field names and values from first market
    if args.raw:
        print("=== Raw fields of first market object ===")
        first = markets[0]
        for k, v in sorted(first.items()):
            print(f"  {k!r}: {v!r}")
        print()

    # Categorize unique tickers by prefix to see what we got
    prefixes: dict[str, int] = {}
    for m in markets:
        ticker = m.get("ticker", "")
        prefix = ticker.split("-")[0] if "-" in ticker else ticker[:8]
        prefixes[prefix] = prefixes.get(prefix, 0) + 1
    if prefixes:
        print("=== Ticker prefix breakdown ===")
        for prefix, count in sorted(prefixes.items(), key=lambda x: -x[1])[:15]:
            print(f"  {prefix:<30} {count:>4} markets")
        print()

    # Show what category field the API reports on the markets themselves
    api_categories: dict[str, int] = {}
    for m in markets:
        cat = m.get("category") or m.get("event_category") or "(none)"
        api_categories[cat] = api_categories.get(cat, 0) + 1
    if any(k != "(none)" for k in api_categories):
        print("=== API-reported category breakdown ===")
        for cat, count in sorted(api_categories.items(), key=lambda x: -x[1]):
            print(f"  {cat:<30} {count:>4} markets")
        print()

    # Volume field investigation
    volume_fields = ["volume_24h_fp", "volume_fp", "volume_24h", "volume", "liquidity_dollars"]
    print("=== Volume field presence across markets ===")
    for field in volume_fields:
        non_zero = sum(1 for m in markets if safe_float(m.get(field), 0) > 0)
        present = sum(1 for m in markets if field in m)
        print(f"  {field:<20} present={present:>3}  non_zero={non_zero:>3}")
    print()

    # Filter breakdown
    n_not_binary = 0
    n_low_volume = 0
    n_low_spread = 0
    n_passed = 0

    binary_markets = []
    for m in markets:
        if not is_supported_binary_market(m):
            n_not_binary += 1
            continue
        binary_markets.append(m)

    volumes = []
    spreads = []
    for m in binary_markets:
        vol = safe_float(m.get("volume_24h", m.get("volume", 0)))
        spread = compute_spread_cents(m)
        volumes.append(vol)
        spreads.append(spread)

        low_vol = vol < args.min_volume
        low_spread = spread < args.min_spread
        if low_vol:
            n_low_volume += 1
        elif low_spread:
            n_low_spread += 1
        else:
            n_passed += 1

    print("=== Filter breakdown ===")
    print(f"  Rejected (not binary / MVE):  {n_not_binary}")
    print(f"  Rejected (volume_24h < {args.min_volume:.0f}):  {n_low_volume}")
    print(f"  Rejected (spread < {args.min_spread:.0f}c):      {n_low_spread}")
    print(f"  Passed all filters:           {n_passed}")
    print()

    if volumes:
        print("=== Volume stats (binary markets only) ===")
        print(f"  min:    {min(volumes):.0f}")
        print(f"  max:    {max(volumes):.0f}")
        print(f"  median: {statistics.median(volumes):.0f}")
        nonzero_vols = [v for v in volumes if v > 0]
        if nonzero_vols:
            print(f"  non-zero count: {len(nonzero_vols)}/{len(volumes)}")
            print(f"  non-zero median: {statistics.median(nonzero_vols):.0f}")
        print()

    if spreads:
        no_quote = sum(1 for s in spreads if s < 0)
        valid_spreads = [s for s in spreads if s >= 0]
        if no_quote == len(spreads):
            print(f"=== Spread stats: all {no_quote} markets have no bid/ask (no_quote) ===\n")
        else:
            print("=== Spread stats (binary markets with quotes) ===")
            print(f"  no_quote: {no_quote}/{len(spreads)}")
            if valid_spreads:
                print(f"  min:    {min(valid_spreads):.1f}c")
                print(f"  max:    {max(valid_spreads):.1f}c")
                print(f"  median: {statistics.median(valid_spreads):.1f}c")
            print()

    print("=== Sample markets (first 5 binary) ===")
    for m in binary_markets[:5]:
        ticker = m.get("ticker", "?")
        vol = safe_float(m.get("volume_24h", m.get("volume", 0)))
        spread = compute_spread_cents(m)
        bid = m.get("yes_bid", "?")
        ask = m.get("yes_ask", "?")
        spread_str = f"{spread:.1f}c" if spread >= 0 else "-1.0c"
        bid_str = str(bid) if bid != "?" and bid is not None else "?"
        ask_str = str(ask) if ask != "?" and ask is not None else "?"
        print(f"  {ticker:<50} vol={vol:>6.0f}  spread={spread_str:>6}  bid={bid_str}  ask={ask_str}")

    print()
    if n_passed == 0:
        print("=== Diagnosis summary ===")
        if n_not_binary == len(markets):
            print("  ALL markets rejected as non-binary/MVE — check market_type field.")
        elif n_low_volume == len(binary_markets):
            nonzero = sum(1 for v in volumes if v > 0)
            if nonzero == 0:
                print("  ALL binary markets have zero volume.")
                print("  Likely causes:")
                print("    1. Wrong category — these markets may not be the ones with activity.")
                print("       Try: python3 scripts/diagnose_markets.py --no-category")
                print("       Or try a different --category value.")
                print("    2. The volume_24h field may be named differently in the API response.")
                print("       Try: python3 scripts/diagnose_markets.py --raw")
                print("    3. These markets are genuinely illiquid right now.")
            else:
                print(f"  {nonzero}/{len(binary_markets)} markets have non-zero volume, but all below threshold {args.min_volume:.0f}.")
                print(f"  Consider lowering --min-volume (e.g. --min-volume 50).")
        elif n_low_spread == len(binary_markets) - n_low_volume:
            print("  Volume is OK but all spreads are too tight or missing.")
            print("  Consider lowering --min-spread.")
        print()


if __name__ == "__main__":
    main()
