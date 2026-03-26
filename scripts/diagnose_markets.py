"""
Diagnostic script: shows why markets are being filtered out.
Usage: python scripts/diagnose_markets.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from kalshi_market_maker.factories import create_api
from kalshi_market_maker.logging_utils import build_logger
from kalshi_market_maker.selection.scoring import (
    compute_spread_cents,
    is_supported_binary_market,
    safe_float,
)

logger = build_logger("Diagnose", "INFO")

api = create_api({}, logger, market_ticker="DIAGNOSTIC")

print("\nFetching sports markets (first page)...\n")
markets = api.list_all_open_markets(
    category="sports",
    mve_filter="exclude",
    page_limit=50,
    max_pages=1,
    max_markets=50,
)

print(f"Total fetched: {len(markets)}\n")

# Tally rejection reasons
rejected_not_binary = 0
rejected_volume = 0
rejected_spread = 0
passed = 0

MIN_VOLUME = 500
MIN_SPREAD = 2

volume_samples = []
spread_samples = []

for m in markets:
    ticker = m.get("ticker", "?")

    if not is_supported_binary_market(m):
        rejected_not_binary += 1
        continue

    vol = safe_float(m.get("volume_24h", m.get("volume", 0)))
    spread = compute_spread_cents(m)
    volume_samples.append(vol)
    spread_samples.append(spread)

    if vol < MIN_VOLUME:
        rejected_volume += 1
    elif spread < MIN_SPREAD:
        rejected_spread += 1
    else:
        passed += 1

print("=== Filter breakdown ===")
print(f"  Rejected (not binary / MVE):  {rejected_not_binary}")
print(f"  Rejected (volume_24h < {MIN_VOLUME}):  {rejected_volume}")
print(f"  Rejected (spread < {MIN_SPREAD}c):      {rejected_spread}")
print(f"  Passed all filters:           {passed}")

if volume_samples:
    print(f"\n=== Volume stats (binary markets only) ===")
    print(f"  min:    {min(volume_samples):.0f}")
    print(f"  max:    {max(volume_samples):.0f}")
    print(f"  median: {sorted(volume_samples)[len(volume_samples)//2]:.0f}")

valid_spreads = [s for s in spread_samples if s >= 0]
if valid_spreads:
    print(f"\n=== Spread stats (binary markets only) ===")
    print(f"  min:    {min(valid_spreads):.1f}c")
    print(f"  max:    {max(valid_spreads):.1f}c")
    print(f"  median: {sorted(valid_spreads)[len(valid_spreads)//2]:.1f}c")
elif spread_samples:
    print(f"\n=== Spread stats: all {len(spread_samples)} markets have no bid/ask (no_quote) ===")

print("\n=== Sample markets (first 5 binary) ===")
shown = 0
for m in markets:
    if not is_supported_binary_market(m):
        continue
    vol = safe_float(m.get("volume_24h", m.get("volume", 0)))
    spread = compute_spread_cents(m)
    print(
        f"  {m.get('ticker','?'):<50} vol={vol:>6.0f}  spread={spread:>5.1f}c  "
        f"bid={m.get('yes_bid','?')}  ask={m.get('yes_ask','?')}"
    )
    shown += 1
    if shown >= 5:
        break
