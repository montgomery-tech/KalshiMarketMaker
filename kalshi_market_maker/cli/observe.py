import argparse
import curses
import time

from dotenv import load_dotenv

from ..config import load_config
from ..factories import create_api, create_market_maker
from ..logging_utils import build_logger


def fetch_market_prices(api, ticker: str):
    """Return raw bid/ask/mid for yes and no sides."""
    data = api.get_market(ticker)
    market = data["market"]
    yes_bid = float(market["yes_bid"]) / 100
    yes_ask = float(market["yes_ask"]) / 100
    no_bid = float(market["no_bid"]) / 100
    no_ask = float(market["no_ask"]) / 100
    yes_mid = round((yes_bid + yes_ask) / 2, 4)
    no_mid = round((no_bid + no_ask) / 2, 4)
    return {
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "yes_mid": yes_mid,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "no_mid": no_mid,
    }


def draw(stdscr, ticker, trade_side, prices, inventory, reservation, our_bid, our_ask, elapsed, T, last_error, refresh):
    stdscr.erase()
    height, width = stdscr.getmaxyx()

    def line(row, text, attr=0):
        if 0 <= row < height:
            stdscr.addstr(row, 0, text[: width - 1].ljust(width - 1), attr)

    line(0, f"A&S Observer  |  {ticker}  |  side={trade_side}  |  refresh={refresh:.1f}s  |  q=quit", curses.A_BOLD)
    line(1, f"Updated: {time.strftime('%Y-%m-%d %H:%M:%S')}   Elapsed: {elapsed:.1f}s / {T:.0f}s")
    line(2, "-" * min(60, width - 1))

    line(4,  "  MARKET PRICES", curses.A_UNDERLINE)
    line(5,  f"    Yes Bid:          {prices['yes_bid']:.4f}  ({prices['yes_bid']*100:.1f}¢)")
    line(6,  f"    Yes Ask:          {prices['yes_ask']:.4f}  ({prices['yes_ask']*100:.1f}¢)")
    line(7,  f"    Yes Mid:          {prices['yes_mid']:.4f}  ({prices['yes_mid']*100:.1f}¢)")
    line(8,  f"    No  Bid:          {prices['no_bid']:.4f}  ({prices['no_bid']*100:.1f}¢)")
    line(9,  f"    No  Ask:          {prices['no_ask']:.4f}  ({prices['no_ask']*100:.1f}¢)")

    mid = prices[f"{trade_side}_mid"]
    line(11, "  A&S MODEL", curses.A_UNDERLINE)
    line(12, f"    Inventory:        {inventory:+d}")
    line(13, f"    Mid Price:        {mid:.4f}  ({mid*100:.1f}¢)")
    line(14, f"    Reservation Price:{reservation:.4f}  ({reservation*100:.1f}¢)  [fair value adjusted for inventory]")
    line(15, f"    Our Bid:          {our_bid:.4f}  ({our_bid*100:.1f}¢)")
    line(16, f"    Our Ask:          {our_ask:.4f}  ({our_ask*100:.1f}¢)")
    line(17, f"    Spread:           {(our_ask - our_bid):.4f}  ({(our_ask - our_bid)*100:.1f}¢)")

    market_bid = prices[f"{trade_side}_bid"]
    market_ask = prices[f"{trade_side}_ask"]
    bid_vs = our_bid - market_bid
    ask_vs = our_ask - market_ask
    line(19, "  VS MARKET", curses.A_UNDERLINE)
    line(20, f"    Our Bid vs Market Bid: {bid_vs:+.4f}  ({bid_vs*100:+.1f}¢)")
    line(21, f"    Our Ask vs Market Ask: {ask_vs:+.4f}  ({ask_vs*100:+.1f}¢)")

    if last_error:
        line(23, f"  ERROR: {last_error}", curses.A_BOLD)

    stdscr.refresh()


def run_observer(stdscr, args, api, mm, ticker, trade_side):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.timeout(200)

    start_time = time.time()
    prices = {}
    inventory = 0
    reservation = 0.0
    our_bid = 0.0
    our_ask = 0.0
    last_error = ""

    # Show connecting screen immediately before the first API call
    height, width = stdscr.getmaxyx()
    stdscr.erase()
    stdscr.addstr(0, 0, f"A&S Observer  |  {ticker}  |  Connecting...", curses.A_BOLD)
    stdscr.addstr(2, 0, "Fetching market data, please wait...")
    stdscr.refresh()

    while True:
        key = stdscr.getch()
        if key in (ord("q"), ord("Q")):
            break

        elapsed = time.time() - start_time

        try:
            prices = fetch_market_prices(api, ticker)
            inventory = api.get_position()
            mid = prices[f"{trade_side}_mid"]
            reservation = mm.calculate_reservation_price(mid, inventory, elapsed)
            our_bid, our_ask = mm.calculate_asymmetric_quotes(mid, inventory, elapsed)
            last_error = ""
        except Exception as exc:
            last_error = str(exc)

        if prices:
            draw(stdscr, ticker, trade_side, prices, inventory, reservation, our_bid, our_ask, elapsed, mm.T, last_error, args.refresh)
        elif last_error:
            stdscr.erase()
            stdscr.addstr(0, 0, f"A&S Observer  |  {ticker}  |  q=quit", curses.A_BOLD)
            stdscr.addstr(2, 0, f"ERROR: {last_error}")
            stdscr.refresh()

        next_fetch = time.time() + max(0.2, args.refresh)
        while time.time() < next_fetch:
            key = stdscr.getch()
            if key in (ord("q"), ord("Q")):
                return
            time.sleep(0.1)


def main():
    parser = argparse.ArgumentParser(description="Read-only A&S model observer — no orders placed")
    parser.add_argument("--ticker", type=str, required=True, help="Kalshi market ticker to observe")
    parser.add_argument("--config", type=str, default="config.yaml", help="Config file for A&S parameters")
    parser.add_argument("--refresh", type=float, default=3.0, help="Refresh interval in seconds")
    parser.add_argument("--log-level", type=str, default="WARNING")
    args = parser.parse_args()

    load_dotenv()
    logger = build_logger("Observe", args.log_level)

    raw_config = load_config(args.config)
    dynamic_config = raw_config.get("dynamic", {})
    mm_config = dynamic_config.get("market_maker", {})
    risk_config = dynamic_config.get("risk", {})

    trade_side = mm_config.get("trade_side", "yes")

    api = create_api({}, logger, market_ticker=args.ticker)
    mm = create_market_maker(mm_config, api, logger, risk_config=risk_config)

    try:
        curses.wrapper(lambda stdscr: run_observer(stdscr, args, api, mm, args.ticker, trade_side))
    finally:
        api.logout()


if __name__ == "__main__":
    main()
