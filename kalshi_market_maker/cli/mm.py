import argparse

from dotenv import load_dotenv

from ..config import get_dynamic_config, load_config
from ..runtime.dynamic import run_dynamic_strategy


def main():
    parser = argparse.ArgumentParser(description="Kalshi Dynamic Market Maker")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to config file")
    parser.add_argument(
        "--tickers",
        type=str,
        nargs="+",
        metavar="TICKER",
        help="One or more market tickers to make markets on directly, skipping the selector",
    )
    args = parser.parse_args()

    load_dotenv()
    raw_config = load_config(args.config)
    dynamic_config = get_dynamic_config(raw_config)

    if args.tickers:
        dynamic_config.setdefault("market_selector", {})["pinned_tickers"] = args.tickers

    run_dynamic_strategy(dynamic_config)


if __name__ == "__main__":
    main()
