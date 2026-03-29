"""
Phase 1 — Descriptive Calibration Analysis
Avellaneda & Stoikov Parameter Estimation

Targets:
  1. Market inventory   → sc_kalshi_market + sc_kalshi_market_stats
  2. Empirical σ²       → sc_kalshi_orderbook_replays + sc_kalshi_pub_trades
  3. Order arrival rate → sc_kalshi_orderbook_messages

Usage:
  python phase1_calibration.py [--output-dir ./output] [--days 90] [--sport MLB]
"""

import os
import json
import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("phase1")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("PROD_ATHENA_HOST"),
    "port":     int(os.getenv("PROD_ATHENA_PORT", 5432)),
    "dbname":   os.getenv("PROD_ATHENA_DB", "postgres"),
    "user":     os.getenv("PROD_ATHENA_USER"),
    "password": os.getenv("PROD_ATHENA_PASSWORD"),
}


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


def query_df(sql: str, params=None) -> pd.DataFrame:
    """Execute SQL and return a DataFrame. Logs row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    df = pd.DataFrame([dict(r) for r in rows])
    log.info("  → %d rows returned", len(df))
    return df


# ---------------------------------------------------------------------------
# Result containers
# ---------------------------------------------------------------------------
@dataclass
class MarketInventoryResult:
    total_markets: int = 0
    by_sport: dict = field(default_factory=dict)
    by_prop_type: dict = field(default_factory=dict)
    volume_stats: dict = field(default_factory=dict)
    lifecycle_stats: dict = field(default_factory=dict)


@dataclass
class VolatilityResult:
    """Empirical σ² estimates segmented by prop type and game phase."""
    by_prop_type: dict = field(default_factory=dict)          # prop_type → σ²
    by_game_phase: dict = field(default_factory=dict)         # phase → σ²
    by_prop_and_phase: dict = field(default_factory=dict)     # (prop, phase) → σ²
    sigma_schedule_recommendation: dict = field(default_factory=dict)
    raw_series_stats: dict = field(default_factory=dict)


@dataclass
class ArrivalRateResult:
    """Empirical order arrival rates (λ) by time-of-game bucket."""
    by_time_bucket: dict = field(default_factory=dict)        # bucket → arrivals/min
    by_sport_and_bucket: dict = field(default_factory=dict)
    peak_activity_windows: list = field(default_factory=list)
    dead_zone_windows: list = field(default_factory=list)
    baseline_A: float = 0.0                                   # overall λ estimate


@dataclass
class Phase1Results:
    run_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    lookback_days: int = 90
    sport_filter: Optional[str] = None
    market_inventory: MarketInventoryResult = field(default_factory=MarketInventoryResult)
    volatility: VolatilityResult = field(default_factory=VolatilityResult)
    arrival_rates: ArrivalRateResult = field(default_factory=ArrivalRateResult)
    config_recommendations: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Phase 1a — Market Inventory
# ---------------------------------------------------------------------------
def run_market_inventory(
    since: datetime,
    sport_filter: Optional[str],
    result: MarketInventoryResult,
) -> None:
    log.info("=== Phase 1a: Market Inventory ===")

    sport_clause = "AND UPPER(m.category) = UPPER(%(sport)s)" if sport_filter else ""
    params = {"since": since, "sport": sport_filter}

    # --- market counts by sport and prop type ---
    log.info("Fetching market counts by sport / prop type...")
    sql = f"""
        SELECT
            m.category                                          AS sport,
            m.event_ticker                                      AS prop_type_raw,
            COUNT(*)                                            AS market_count,
            AVG(ms.volume)                                      AS avg_volume,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ms.volume)
                                                                AS median_volume,
            AVG(ms.open_interest)                               AS avg_open_interest,
            MIN(m.open_time)                                    AS earliest_open,
            MAX(m.close_time)                                   AS latest_close
        FROM sc_kalshi_market m
        LEFT JOIN sc_kalshi_market_stats ms ON ms.market_ticker = m.ticker
        WHERE m.open_time >= %(since)s
          AND m.market_type = 'binary'
          {sport_clause}
        GROUP BY m.category, m.event_ticker
        ORDER BY market_count DESC
    """
    df = query_df(sql, params)

    if df.empty:
        log.warning("No markets found. Check table names or date range.")
        return

    result.total_markets = int(df["market_count"].sum())
    result.by_sport = (
        df.groupby("sport")["market_count"].sum().to_dict()
    )

    # Summarize prop types (top 20)
    result.by_prop_type = (
        df.nlargest(20, "market_count")
        [["prop_type_raw", "market_count", "avg_volume", "median_volume"]]
        .set_index("prop_type_raw")
        .to_dict(orient="index")
    )

    # --- lifecycle duration stats ---
    log.info("Fetching market lifecycle durations...")
    sql_lifecycle = f"""
        SELECT
            m.category                                          AS sport,
            AVG(
                EXTRACT(EPOCH FROM (m.close_time - m.open_time)) / 3600.0
            )                                                   AS avg_duration_hrs,
            PERCENTILE_CONT(0.25) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (m.close_time - m.open_time)) / 3600.0
            )                                                   AS p25_duration_hrs,
            PERCENTILE_CONT(0.75) WITHIN GROUP (
                ORDER BY EXTRACT(EPOCH FROM (m.close_time - m.open_time)) / 3600.0
            )                                                   AS p75_duration_hrs,
            COUNT(*)                                            AS n
        FROM sc_kalshi_market m
        WHERE m.open_time >= %(since)s
          AND m.close_time IS NOT NULL
          {sport_clause}
        GROUP BY m.category
    """
    df_life = query_df(sql_lifecycle, params)
    result.lifecycle_stats = df_life.set_index("sport").to_dict(orient="index")

    # --- volume distribution ---
    log.info("Fetching volume distribution...")
    sql_vol = f"""
        SELECT
            PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY ms.volume) AS p10,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ms.volume) AS p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ms.volume) AS p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ms.volume) AS p75,
            PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ms.volume) AS p90,
            AVG(ms.volume)                                           AS mean,
            STDDEV(ms.volume)                                        AS std
        FROM sc_kalshi_market_stats ms
        JOIN sc_kalshi_market m ON m.ticker = ms.market_ticker
        WHERE m.open_time >= %(since)s
          {sport_clause}
    """
    df_vd = query_df(sql_vol, params)
    result.volume_stats = df_vd.iloc[0].to_dict() if not df_vd.empty else {}

    log.info(
        "Market inventory complete. total_markets=%d sports=%s",
        result.total_markets,
        list(result.by_sport.keys()),
    )


# ---------------------------------------------------------------------------
# Phase 1b — Empirical σ² (mid-price variance)
# ---------------------------------------------------------------------------
def run_volatility_analysis(
    since: datetime,
    sport_filter: Optional[str],
    result: VolatilityResult,
) -> None:
    log.info("=== Phase 1b: Empirical σ² Estimation ===")

    sport_clause = "AND UPPER(m.category) = UPPER(%(sport)s)" if sport_filter else ""
    params = {"since": since, "sport": sport_filter}

    # ------------------------------------------------------------------
    # Step 1: reconstruct mid-price series from orderbook replays
    # ------------------------------------------------------------------
    log.info("Reconstructing mid-price series from orderbook replays...")
    sql = f"""
        SELECT
            r.market_ticker,
            m.category                                          AS sport,
            m.event_ticker                                      AS prop_type,
            r.ts                                                AS snapshot_time,
            -- best bid = highest price on the YES bid side
            r.yes_bid                                           AS best_bid,
            -- best ask = lowest price on the YES ask side
            r.yes_ask                                           AS best_ask,
            (r.yes_bid + r.yes_ask) / 2.0                      AS mid_price
        FROM sc_kalshi_orderbook_replays r
        JOIN sc_kalshi_market m ON m.ticker = r.market_ticker
        WHERE r.ts >= %(since)s
          AND r.yes_bid IS NOT NULL
          AND r.yes_ask IS NOT NULL
          AND r.yes_ask > r.yes_bid          -- valid crossed-book filter
          {sport_clause}
        ORDER BY r.market_ticker, r.ts
    """
    df = query_df(sql, params)

    if df.empty:
        log.warning("No orderbook replay data found. Falling back to pub_trades.")
        df = _fallback_mid_from_trades(since, sport_filter)

    if df.empty:
        log.warning("No price data available for σ² estimation.")
        return

    # ------------------------------------------------------------------
    # Step 2: compute per-market realized variance
    # ------------------------------------------------------------------
    log.info("Computing per-market realized variance...")

    records = []
    for (market_ticker, sport, prop_type), grp in df.groupby(
        ["market_ticker", "sport", "prop_type"]
    ):
        grp = grp.sort_values("snapshot_time").copy()
        grp["mid_price"] = pd.to_numeric(grp["mid_price"], errors="coerce")
        grp = grp.dropna(subset=["mid_price"])

        if len(grp) < 10:
            continue  # not enough observations

        # Time-normalize: convert to seconds since market open
        grp["t_sec"] = (
            pd.to_datetime(grp["snapshot_time"]) - pd.to_datetime(grp["snapshot_time"]).iloc[0]
        ).dt.total_seconds()

        # Game-progress bucket: 0-33% early, 33-66% mid, 66-100% late
        max_t = grp["t_sec"].max()
        grp["phase"] = pd.cut(
            grp["t_sec"] / max(max_t, 1),
            bins=[0, 0.33, 0.66, 1.01],
            labels=["early", "mid", "late"],
        )

        # Realized variance: variance of mid-price changes (in probability units)
        grp["delta_mid"] = grp["mid_price"].diff()
        sigma_sq_full = grp["delta_mid"].var()

        # Per-phase variance
        phase_var = {}
        for phase, pgrp in grp.groupby("phase", observed=True):
            if len(pgrp) >= 5:
                phase_var[str(phase)] = float(pgrp["delta_mid"].var())

        records.append(
            {
                "market_ticker": market_ticker,
                "sport": sport,
                "prop_type": prop_type,
                "n_obs": len(grp),
                "sigma_sq": float(sigma_sq_full),
                "sigma": float(np.sqrt(sigma_sq_full)) if sigma_sq_full > 0 else 0,
                "phase_var": phase_var,
                "duration_sec": max_t,
            }
        )

    if not records:
        log.warning("Could not compute variance for any market.")
        return

    df_var = pd.DataFrame(records)

    # ------------------------------------------------------------------
    # Step 3: aggregate by prop type
    # ------------------------------------------------------------------
    log.info("Aggregating σ² by prop type...")
    agg = (
        df_var.groupby("prop_type")["sigma_sq"]
        .agg(["mean", "median", "std", "count"])
        .rename(columns={"mean": "sigma_sq_mean", "median": "sigma_sq_median",
                         "std": "sigma_sq_std", "count": "n_markets"})
    )
    result.by_prop_type = agg.to_dict(orient="index")

    # ------------------------------------------------------------------
    # Step 4: aggregate by game phase (across all prop types)
    # ------------------------------------------------------------------
    log.info("Aggregating σ² by game phase...")
    phase_records = []
    for rec in records:
        for phase, var in rec["phase_var"].items():
            phase_records.append({"phase": phase, "sigma_sq": var, "prop_type": rec["prop_type"]})

    if phase_records:
        df_phase = pd.DataFrame(phase_records)
        result.by_game_phase = (
            df_phase.groupby("phase")["sigma_sq"]
            .agg(["mean", "median", "std", "count"])
            .to_dict(orient="index")
        )
        result.by_prop_and_phase = (
            df_phase.groupby(["prop_type", "phase"])["sigma_sq"]
            .mean()
            .unstack(fill_value=np.nan)
            .to_dict(orient="index")
        )

    # ------------------------------------------------------------------
    # Step 5: generate sigma schedule recommendation
    # ------------------------------------------------------------------
    median_sigma_sq = float(df_var["sigma_sq"].median())
    result.sigma_schedule_recommendation = {
        "pre_game":  round(float(df_phase[df_phase["phase"] == "early"]["sigma_sq"].median()), 6)
                     if phase_records else median_sigma_sq,
        "in_game":   round(float(df_phase[df_phase["phase"] == "mid"]["sigma_sq"].median()), 6)
                     if phase_records else median_sigma_sq,
        "late_game": round(float(df_phase[df_phase["phase"] == "late"]["sigma_sq"].median()), 6)
                     if phase_records else median_sigma_sq,
        "overall_median_sigma_sq": round(median_sigma_sq, 6),
        "overall_median_sigma":    round(float(np.sqrt(median_sigma_sq)), 6),
        "note": (
            "Use pre_game sigma for T calibration. "
            "Consider a dynamic sigma that decays from pre_game → late_game."
        ),
    }

    result.raw_series_stats = {
        "total_markets_analyzed": len(df_var),
        "median_observations_per_market": float(df_var["n_obs"].median()),
        "median_market_duration_sec": float(df_var["duration_sec"].median()),
    }

    log.info(
        "Volatility analysis complete. overall_median_sigma=%.5f",
        result.sigma_schedule_recommendation["overall_median_sigma"],
    )


def _fallback_mid_from_trades(since: datetime, sport_filter: Optional[str]) -> pd.DataFrame:
    """Fallback: estimate mid from trade prices when no orderbook data available."""
    log.info("Using pub_trades as fallback for mid-price series...")
    sport_clause = "AND UPPER(m.category) = UPPER(%(sport)s)" if sport_filter else ""
    sql = f"""
        SELECT
            t.market_ticker,
            m.category      AS sport,
            m.event_ticker  AS prop_type,
            t.created_time  AS snapshot_time,
            t.yes_price     AS mid_price
        FROM sc_kalshi_pub_trades t
        JOIN sc_kalshi_market m ON m.ticker = t.market_ticker
        WHERE t.created_time >= %(since)s
          {sport_clause}
        ORDER BY t.market_ticker, t.created_time
    """
    return query_df(sql, {"since": since, "sport": sport_filter})


# ---------------------------------------------------------------------------
# Phase 1c — Order Arrival Rates (λ / baseline A)
# ---------------------------------------------------------------------------
def run_arrival_rate_analysis(
    since: datetime,
    sport_filter: Optional[str],
    result: ArrivalRateResult,
) -> None:
    log.info("=== Phase 1c: Order Arrival Rate Estimation ===")

    sport_clause = "AND UPPER(m.category) = UPPER(%(sport)s)" if sport_filter else ""
    params = {"since": since, "sport": sport_filter}

    # ------------------------------------------------------------------
    # Step 1: message counts by time-of-game bucket
    # ------------------------------------------------------------------
    log.info("Fetching orderbook message counts by time bucket...")
    sql = f"""
        SELECT
            m.category                              AS sport,
            msg.market_ticker,
            msg.ts                                  AS msg_time,
            m.open_time,
            -- normalized game progress: 0.0 (open) → 1.0 (close)
            CASE
                WHEN m.close_time IS NOT NULL AND m.close_time > m.open_time THEN
                    LEAST(1.0,
                        EXTRACT(EPOCH FROM (msg.ts - m.open_time))
                        / NULLIF(EXTRACT(EPOCH FROM (m.close_time - m.open_time)), 0)
                    )
                ELSE NULL
            END                                     AS game_progress,
            msg.type                                AS message_type
        FROM sc_kalshi_orderbook_messages msg
        JOIN sc_kalshi_market m ON m.ticker = msg.market_ticker
        WHERE msg.ts >= %(since)s
          {sport_clause}
    """
    df = query_df(sql, params)

    if df.empty:
        log.warning("No orderbook message data. Trying pub_trades for arrival proxy...")
        df = _arrival_fallback_trades(since, sport_filter)

    if df.empty:
        log.warning("No arrival rate data available.")
        return

    df["game_progress"] = pd.to_numeric(df["game_progress"], errors="coerce")
    df = df.dropna(subset=["game_progress"])

    # ------------------------------------------------------------------
    # Step 2: bucket game_progress into 10 equal bins
    # ------------------------------------------------------------------
    bins = np.linspace(0, 1, 11)
    labels = [f"{int(b*100)}-{int(bins[i+1]*100)}%" for i, b in enumerate(bins[:-1])]
    df["bucket"] = pd.cut(df["game_progress"], bins=bins, labels=labels, include_lowest=True)

    # Arrivals per minute per market per bucket
    # We need the time width of each bucket per market to normalize
    log.info("Computing arrivals per minute by bucket...")

    # Estimate average market duration per sport
    sql_dur = f"""
        SELECT
            m.category AS sport,
            AVG(EXTRACT(EPOCH FROM (m.close_time - m.open_time))) AS avg_duration_sec
        FROM sc_kalshi_market m
        WHERE m.open_time >= %(since)s
          AND m.close_time IS NOT NULL
          {sport_clause}
        GROUP BY m.category
    """
    df_dur = query_df(sql_dur, params)
    avg_duration = (
        float(df_dur["avg_duration_sec"].mean()) if not df_dur.empty else 10800.0
    )  # default 3hr

    bucket_width_sec = avg_duration / 10.0  # each bucket = 10% of game time
    bucket_width_min = bucket_width_sec / 60.0

    # Count messages per bucket across all markets, normalize by market count
    n_markets = df["market_ticker"].nunique()
    bucket_counts = df.groupby("bucket", observed=True).size()
    arrivals_per_min = (bucket_counts / n_markets / bucket_width_min).to_dict()

    result.by_time_bucket = {str(k): round(float(v), 4) for k, v in arrivals_per_min.items()}

    # ------------------------------------------------------------------
    # Step 3: by sport
    # ------------------------------------------------------------------
    if "sport" in df.columns:
        sport_bucket = (
            df.groupby(["sport", "bucket"], observed=True)
            .size()
            .reset_index(name="count")
        )
        sport_markets = df.groupby("sport")["market_ticker"].nunique().to_dict()
        for (sport, bucket), cnt in sport_bucket.set_index(["sport", "bucket"])["count"].items():
            n_m = sport_markets.get(sport, 1)
            rate = cnt / n_m / bucket_width_min
            key = f"{sport}_{bucket}"
            result.by_sport_and_bucket[key] = round(float(rate), 4)

    # ------------------------------------------------------------------
    # Step 4: identify peak and dead zones
    # ------------------------------------------------------------------
    if arrivals_per_min:
        sorted_buckets = sorted(arrivals_per_min.items(), key=lambda x: x[1], reverse=True)
        overall_mean = float(np.mean(list(arrivals_per_min.values())))

        result.peak_activity_windows = [
            {"bucket": str(b), "arrivals_per_min": round(float(r), 4)}
            for b, r in sorted_buckets
            if float(r) > overall_mean * 1.5
        ]
        result.dead_zone_windows = [
            {"bucket": str(b), "arrivals_per_min": round(float(r), 4)}
            for b, r in sorted_buckets
            if float(r) < overall_mean * 0.5
        ]

        # Baseline A = median arrival rate across active windows
        active_rates = [v for v in arrivals_per_min.values() if v > 0]
        result.baseline_A = round(float(np.median(active_rates)), 4) if active_rates else 0.0

    log.info(
        "Arrival rate analysis complete. baseline_A=%.4f arrivals/min",
        result.baseline_A,
    )


def _arrival_fallback_trades(since: datetime, sport_filter: Optional[str]) -> pd.DataFrame:
    sport_clause = "AND UPPER(m.category) = UPPER(%(sport)s)" if sport_filter else ""
    sql = f"""
        SELECT
            t.market_ticker,
            m.category  AS sport,
            t.created_time AS ts,
            m.open_time,
            m.close_time,
            CASE
                WHEN m.close_time IS NOT NULL AND m.close_time > m.open_time THEN
                    LEAST(1.0,
                        EXTRACT(EPOCH FROM (t.created_time - m.open_time))
                        / NULLIF(EXTRACT(EPOCH FROM (m.close_time - m.open_time)), 0)
                    )
                ELSE NULL
            END AS game_progress,
            'trade' AS message_type
        FROM sc_kalshi_pub_trades t
        JOIN sc_kalshi_market m ON m.ticker = t.market_ticker
        WHERE t.created_time >= %(since)s
          {sport_clause}
    """
    return query_df(sql, {"since": since, "sport": sport_filter})


# ---------------------------------------------------------------------------
# Config recommendations
# ---------------------------------------------------------------------------
def build_recommendations(results: Phase1Results) -> dict:
    rec = {}
    vol = results.volatility
    arr = results.arrival_rates
    inv = results.market_inventory

    # Sigma recommendation
    sched = vol.sigma_schedule_recommendation
    if sched:
        rec["sigma"] = {
            "current_config": 0.10,
            "recommended": round(sched.get("overall_median_sigma", 0.10), 5),
            "rationale": (
                "Empirically derived from historical mid-price variance. "
                f"Pre-game: {sched.get('pre_game','?')}, "
                f"In-game: {sched.get('in_game','?')}, "
                f"Late-game: {sched.get('late_game','?')}."
            ),
        }

    # T recommendation (active information time)
    median_dur = inv.lifecycle_stats
    if median_dur:
        sample_sport = next(iter(median_dur))
        avg_hrs = median_dur[sample_sport].get("avg_duration_hrs", 3.0)
        active_fraction = 0.4  # heuristic: ~40% of market lifetime is active
        rec["T"] = {
            "current_config": 28800,
            "recommended_wall_clock": int(avg_hrs * 3600),
            "recommended_active_time": int(avg_hrs * 3600 * active_fraction),
            "rationale": (
                f"Average market duration is ~{avg_hrs:.1f}hrs. "
                "Replace wall-clock T with active information time (~40% of duration). "
                "Implement as a game-progress scalar rather than a fixed constant."
            ),
        }

    # k (kappa) recommendation — Phase 2 will compute directly
    if arr.baseline_A:
        rec["k"] = {
            "current_config": 150.0,
            "note": (
                "k=150 implies fill probability is nearly insensitive to spread width — "
                "implausible for illiquid prediction markets. "
                f"Baseline arrival rate A={arr.baseline_A:.4f} arrivals/min. "
                "Run Phase 2 fill-rate analysis to compute k empirically. "
                "Expected range for Kalshi props: 5–30."
            ),
        }

    # order_expiration recommendation
    if arr.dead_zone_windows:
        rec["order_expiration"] = {
            "current_config": 3600,
            "recommended": 300,
            "rationale": (
                "Dead zones identified in order flow suggest market conditions change "
                "faster than a 1-hour TTL. Recommend 60–300s and tie expiration "
                "to your quote refresh cycle."
            ),
        }

    return rec


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def save_results(results: Phase1Results, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    # Full JSON dump
    json_path = output_dir / f"phase1_results_{ts}.json"
    with open(json_path, "w") as f:
        json.dump(asdict(results), f, indent=2, default=str)
    log.info("Full results saved → %s", json_path)

    # Human-readable summary
    summary_path = output_dir / f"phase1_summary_{ts}.txt"
    with open(summary_path, "w") as f:
        _write_summary(results, f)
    log.info("Summary saved → %s", summary_path)


def _write_summary(results: Phase1Results, f):
    def h(title):
        f.write(f"\n{'='*60}\n{title}\n{'='*60}\n")

    f.write(f"Phase 1 Calibration Analysis\n")
    f.write(f"Run at:      {results.run_at}\n")
    f.write(f"Lookback:    {results.lookback_days} days\n")
    f.write(f"Sport:       {results.sport_filter or 'ALL'}\n")

    h("1. MARKET INVENTORY")
    inv = results.market_inventory
    f.write(f"Total markets:   {inv.total_markets}\n")
    f.write(f"By sport:        {json.dumps(inv.by_sport, indent=2)}\n")
    if inv.volume_stats:
        vs = inv.volume_stats
        f.write(
            f"Volume (p10/p50/p90): "
            f"{vs.get('p10','?'):.1f} / {vs.get('p50','?'):.1f} / {vs.get('p90','?'):.1f}\n"
        )
    if inv.lifecycle_stats:
        f.write("Market lifecycle durations (avg hrs):\n")
        for sport, stats in inv.lifecycle_stats.items():
            f.write(f"  {sport}: {stats.get('avg_duration_hrs', '?'):.2f} hrs\n")

    h("2. EMPIRICAL σ² (MID-PRICE VARIANCE)")
    vol = results.volatility
    sched = vol.sigma_schedule_recommendation
    if sched:
        f.write(f"Overall median σ:    {sched.get('overall_median_sigma', '?')}\n")
        f.write(f"Overall median σ²:   {sched.get('overall_median_sigma_sq', '?')}\n")
        f.write(f"Pre-game σ²:         {sched.get('pre_game', '?')}\n")
        f.write(f"In-game σ²:          {sched.get('in_game', '?')}\n")
        f.write(f"Late-game σ²:        {sched.get('late_game', '?')}\n")
    if vol.by_prop_type:
        f.write("\nσ² by prop type (top 10 by market count):\n")
        for prop, stats in list(vol.by_prop_type.items())[:10]:
            f.write(
                f"  {prop:40s}  σ²_median={stats.get('median','?'):.6f}  "
                f"n={stats.get('count','?')}\n"
            )

    h("3. ORDER ARRIVAL RATES")
    arr = results.arrival_rates
    f.write(f"Baseline A (median arrivals/min):  {arr.baseline_A:.4f}\n")
    if arr.by_time_bucket:
        f.write("\nArrivals/min by game progress bucket:\n")
        for bucket, rate in arr.by_time_bucket.items():
            bar = "█" * int(rate * 10)
            f.write(f"  {bucket:12s}  {rate:6.3f}  {bar}\n")
    if arr.peak_activity_windows:
        f.write(f"\nPeak windows:     {[w['bucket'] for w in arr.peak_activity_windows]}\n")
    if arr.dead_zone_windows:
        f.write(f"Dead zones:       {[w['bucket'] for w in arr.dead_zone_windows]}\n")

    h("4. CONFIG RECOMMENDATIONS")
    for param, rec in results.config_recommendations.items():
        f.write(f"\n[{param}]\n")
        for k, v in rec.items():
            f.write(f"  {k}: {v}\n")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Phase 1 A&S Calibration Analysis")
    parser.add_argument("--output-dir", default="./output", type=Path)
    parser.add_argument("--days", default=90, type=int, help="Lookback window in days")
    parser.add_argument("--sport", default=None, type=str, help="Filter to sport (e.g. MLB, NBA)")
    args = parser.parse_args()

    since = datetime.now(timezone.utc) - timedelta(days=args.days)

    results = Phase1Results(
        lookback_days=args.days,
        sport_filter=args.sport,
    )

    run_market_inventory(since, args.sport, results.market_inventory)
    run_volatility_analysis(since, args.sport, results.volatility)
    run_arrival_rate_analysis(since, args.sport, results.arrival_rates)

    results.config_recommendations = build_recommendations(results)

    save_results(results, args.output_dir)

    log.info("=== Phase 1 Complete ===")
    log.info("Key findings:")
    sched = results.volatility.sigma_schedule_recommendation
    if sched:
        log.info("  σ (empirical):     %.5f  (config has 0.10000)", sched.get("overall_median_sigma", 0))
    if results.arrival_rates.baseline_A:
        log.info("  A (arrivals/min):  %.4f", results.arrival_rates.baseline_A)


if __name__ == "__main__":
    main()
