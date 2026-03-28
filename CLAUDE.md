# Claude Code Instructions

## Git Workflow

**Before writing any code**, run these commands to understand the current state:
```bash
git branch -a
git log --oneline -10
git status
```

**Always develop on the existing feature branch** — do NOT create a new branch unless explicitly asked. Check which branch has the latest work:
```bash
git log --oneline origin/claude/add-recent-trades-data-4mLom -5
```

**Never leave work uncommitted.** Every session must end with all changes committed and pushed.

**Before creating any file**, check if it already exists on any branch:
```bash
git log --all --oneline -- kalshi_market_maker/cli/<filename>.py
```

**When fixing a bug**, search all branches first to see if it was already fixed elsewhere:
```bash
git log --all --oneline --grep="<keyword>"
```

---

## Project Structure

```
kalshi_market_maker/
  cli/
    mm.py            # kalshi-mm: dynamic market maker bot
    dashboard.py     # kalshi-dashboard: live account view (positions, orders, balance)
    observe.py       # kalshi-observe --ticker X: A&S model observer + recent trades
    list_sports.py   # kalshi-list-sports: list live sports contracts
    cancel_all.py    # kalshi-cancel-all: cancel orders and liquidate
  core/
    kalshi_api.py    # Kalshi REST API client
    avellaneda.py    # Avellaneda-Stoikov market making algorithm
  runtime/
    dynamic.py       # Multi-market selector loop + worker management
    workers.py       # Per-market worker thread
    cleanup.py       # Graceful shutdown / order cancellation
  selection/
    scoring.py       # Market ranking by volume + spread
config.yaml          # Runtime configuration
```

---

## Known API Behaviour

- `GET /markets` — The `category` query param is **silently ignored** by the Kalshi API. Do NOT use it for filtering. Use `series_ticker` to narrow to a specific series, or filter client-side.
- Market prices are returned as `yes_bid_dollars`, `yes_ask_dollars`, `no_bid_dollars`, `no_ask_dollars` (float, already in dollars). Do NOT use `yes_bid / 100`.
- `GET /markets/{ticker}/trades` — returns recent trades. Some settled markets return 404; handle gracefully.
- Retryable HTTP status codes: `429, 500, 502, 503, 504` only. Do NOT retry 4xx errors.

---

## Config Notes

- `min_volume_24h: 10` — sports markets have lower volume than general markets; 500 is too high
- `pinned_tickers` — uncomment in config.yaml to bypass the selector and trade specific markets directly
- `series_ticker` — use this (e.g. `KXNBA`) to narrow market selection to a specific sport

---

## CLI Entry Points

| Command | File | Purpose |
|---------|------|---------|
| `kalshi-mm` | `cli/mm.py` | Run the market maker |
| `kalshi-dashboard` | `cli/dashboard.py` | Live account dashboard |
| `kalshi-observe --ticker X` | `cli/observe.py` | Observe a single market |
| `kalshi-list-sports` | `cli/list_sports.py` | List live sports contracts |
| `kalshi-cancel-all` | `cli/cancel_all.py` | Cancel all orders |

---

## Before Ending a Session

1. Commit all changes with a descriptive message
2. Push to the current branch
3. Do NOT leave local-only files that aren't pushed
