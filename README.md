# TradingBot

Algorithmic trading system for NSE (India) with 14 strategies, backtesting, paper trading with virtual money, and live execution via Dhan / Zerodha.

## Quick Start

```bash
# 1. Clone & setup
git clone https://github.com/Ayush1906saxena/TradingBot.git
cd TradingBot
bash setup.sh            # creates venv, installs deps, creates .env

# 2. Activate venv
source venv/bin/activate

# 3. Run backtest (works immediately, no API keys needed)
python main.py --mode backtest
```

Backtest downloads free daily data from Yahoo Finance for 5 NSE stocks (RELIANCE, TCS, HDFCBANK, INFY, ICICIBANK) and runs all enabled strategies. HTML reports with equity curves are saved to `logs/`.

## Modes

| Mode | Command | What it does |
|------|---------|--------------|
| **Backtest** | `python main.py --mode backtest` | Simulate strategies on historical data. No API keys needed. |
| **Paper** | `python main.py --mode paper` | Trade with virtual money (Rs 1,00,000) using live market data. Needs Dhan API. |
| **Live** | `python main.py --mode live` | Real orders with real money. Needs Dhan/Zerodha API. |
| **Dashboard** | `python main.py --mode dashboard` | Streamlit web UI on localhost:8501. |

## Setting Up Dhan API (Free)

Paper and live modes need a Dhan account with the free Trading API:

1. Open a Dhan account at [dhan.co](https://dhan.co)
2. Log in at [web.dhan.co](https://web.dhan.co) (choose "Dhan Web" when prompted)
3. Go to **Profile** (top right) > **DhanHQ Trading APIs**
4. Click **Request Access** (first time only, instant approval)
5. Click **Generate Access Token** — copy the token and note your Client ID

Add credentials using **either** method:

**Option A — `.env` file** (recommended, git-ignored):
```bash
cp .env.example .env
# Edit .env:
DHAN_CLIENT_ID=your_client_id_here
DHAN_ACCESS_TOKEN=your_access_token_here
```

**Option B — `configs/config.yaml`**:
```yaml
broker:
  dhan:
    client_id: "your_client_id_here"
    access_token: "your_access_token_here"
```

> **Note:** The access token expires daily. You'll need to regenerate it each morning before market opens. The system uses Dhan's free Market Quote API (polling LTP every second) — no paid Data API subscription (Rs 499/mo) needed.

## Paper Trading

```bash
# Start with default Rs 1,00,000 virtual cash
python main.py --mode paper

# Or specify custom virtual capital
python main.py --mode paper --virtual-cash 500000

# Reset virtual wallet to start fresh
python main.py --mode paper --reset-virtual-wallet
```

The system auto-schedules everything during market hours (IST):
- **08:45** — Pre-market setup, download instrument data
- **09:15** — Connect to market data, warm up candle history
- **09:20** — Start generating signals
- **15:15** — Stop new signals
- **15:20** — Force-close all intraday positions
- **15:30** — End-of-day report
- **16:00** — Update historical database

Just start it before 9:15 AM and leave it running.

## Strategies (14)

| # | Strategy | Type | Description |
|---|----------|------|-------------|
| 1 | SMA Crossover | Trend | 9/21 SMA crossover |
| 2 | RSI Reversal | Mean reversion | Buy RSI<30, sell RSI>70 |
| 3 | EMA + RSI + Volume | Trend | Triple confirmation with 3-phase trailing stop |
| 4 | Supertrend | Trend | ATR-based trend following |
| 5 | MACD Crossover | Momentum | MACD/signal line crossover |
| 6 | Bollinger Bands | Mean reversion | Bounce off bands, target = middle band |
| 7 | VWAP | Intraday | Price crossing VWAP with volume confirmation |
| 8 | Donchian Channel | Breakout | Turtle Trading 20-period breakout system |
| 9 | Stochastic Oscillator | Momentum | %K/%D crossover in OS/OB zones |
| 10 | ADX Trend | Trend | +DI/-DI crossover when ADX > 25 |
| 11 | Ichimoku Cloud | Trend | Tenkan-Kijun cross confirmed by cloud |
| 12 | Mean Reversion Z-Score | Statistical | Z-score entry at +/-2, exit at mean |
| 13 | Momentum ROC | Momentum | Rate of Change threshold crossover |
| 14 | Parabolic SAR | Trend | SAR flip with ADX trend filter |

Enable/disable and configure each strategy in `configs/config.yaml` under the `strategies:` section.

## Project Structure

```
TradingBot/
├── main.py                  # Entry point (--mode backtest/paper/live/dashboard)
├── configs/
│   └── config.yaml          # All settings: strategies, capital, broker, alerts
├── strategies/
│   ├── base_strategy.py     # ABC that all strategies inherit from
│   ├── sma_crossover.py     # Strategy 1
│   ├── rsi_reversal.py      # Strategy 2
│   ├── ...                  # Strategies 3-14
│   └── parabolic_sar.py     # Strategy 14
├── engine/
│   ├── backtester.py        # Historical simulation engine
│   ├── paper_trader.py      # Paper + live trading engine (scheduler, candle heartbeat)
│   └── live_trader.py       # Re-exports TradingEngine for live mode
├── data/
│   ├── historical.py        # Yahoo Finance (daily) + Dhan API (minute) data fetcher
│   ├── polling_stream.py    # FREE polling data feed (Market Quote API, 1 req/sec)
│   ├── live_stream.py       # WebSocket data feed (needs paid Data API)
│   └── instruments.py       # Symbol-to-security-ID mapping (Dhan scrip master)
├── broker/
│   ├── base_gateway.py      # ABC for broker integrations
│   ├── dhan_gateway.py      # Dhan order placement
│   └── zerodha_gateway.py   # Zerodha/Kite Connect order placement
├── risk/
│   ├── risk_manager.py      # Position sizing, daily loss limits, kill switch
│   └── virtual_portfolio.py # Virtual wallet for paper trading
├── orders/
│   └── order_manager.py     # Order execution, fee calculation (realistic NSE fees)
├── monitoring/
│   ├── telegram_bot.py      # Telegram trade alerts
│   └── daily_report.py      # End-of-day P&L summary
├── dashboard/
│   └── app.py               # Streamlit 5-page web dashboard
├── db/
│   └── __init__.py          # SQLite schema (8 tables), init_db(), get_connection()
├── utils/
│   ├── constants.py         # Market hours, NSE holidays, fee constants
│   ├── helpers.py           # IST timezone, rounding, formatting utilities
│   └── logger.py            # Rotating file logger setup
├── tests/                   # 36 tests (pytest)
├── .env.example             # Template for API secrets
├── setup.sh                 # One-command macOS setup
└── requirements.txt         # Python dependencies
```

## Configuration

All configuration lives in `configs/config.yaml`. Key sections:

- **`capital`** — Total capital, max risk per trade (2%), daily loss limit (5%), max positions
- **`strategies`** — Enable/disable strategies, set symbols, timeframes, and parameters
- **`backtest`** — Date range, slippage, commission settings
- **`paper_trading`** — Virtual cash amount, slippage/fee simulation toggles
- **`telegram`** — Bot token and chat ID for trade alerts
- **`market`** — Trading hours, force-exit time

## Risk Management

Built-in safeguards (all configurable):

- **2% risk per trade** — Position size calculated from stop-loss distance
- **5% daily loss limit** — All positions force-closed, trading halted for the day
- **5 consecutive loss kill switch** — Trading halted until manual reset
- **Max 5 open positions** — Prevents overexposure
- **20% per stock cap** — No single stock gets more than 20% of capital
- **80% deployment cap** — Always keeps 20% as cash buffer
- **Realistic fee simulation** — Brokerage, STT, GST, stamp duty, SEBI charges

Reset the kill switch: `python main.py --mode paper --reset-kill-switch`

## Backtest Results

Run a backtest to see HTML reports with equity curves and trade-by-trade breakdown:

```bash
python main.py --mode backtest
# Reports saved to logs/backtest_<strategy>_<timestamp>.html
```

Modify backtest date range in `configs/config.yaml`:
```yaml
backtest:
  start_date: "2024-01-01"
  end_date: "2025-12-31"
```

## Telegram Alerts (Optional)

1. Open Telegram, search `@BotFather`, send `/newbot`, follow prompts
2. Copy the bot token
3. Search `@userinfobot`, send `/start`, copy the `Id` number
4. Start a chat with your new bot (search by name, click Start)
5. Add to `.env`:
   ```
   TELEGRAM_BOT_TOKEN=your_bot_token
   TELEGRAM_CHAT_ID=your_chat_id
   ```
6. Set `telegram.enabled: true` in `config.yaml`

## Adding a New Strategy

1. Create `strategies/my_strategy.py`:
   ```python
   from strategies.base_strategy import BaseStrategy

   class MyStrategy(BaseStrategy):
       def __init__(self, config: dict):
           super().__init__("my_strategy", config)
           # read params from config

       def compute_indicators(self, df):
           df = df.copy()
           # add columns to df
           return df

       def generate_signal(self, df, symbol):
           # return {"action": "BUY"/"SELL", "symbol": ..., "price": ...,
           #         "stop_loss": ..., "target": ..., "strategy": self.name,
           #         "reason": "..."} or None
   ```

2. Register it in `engine/backtester.py` (`_load_strategies`) and `engine/paper_trader.py` (`_init_strategies`)

3. Add config block in `configs/config.yaml` under `strategies:`

4. Run backtest to validate: `python main.py --mode backtest`

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError: pandas_ta` | `pip install pandas_ta` or re-run `setup.sh` |
| Backtest returns 0 trades | Check strategy is `enabled: true` in config.yaml |
| Paper mode: "credentials not set" | Add Dhan client_id and access_token to `.env` or config.yaml |
| Access token expired | Regenerate daily at [web.dhan.co](https://web.dhan.co) > Profile > DhanHQ Trading APIs |
| Kill switch active | Run with `--reset-kill-switch` flag |
| Mac sleeping during trading | Run `caffeinate -d -t 28800 &` or adjust Energy settings |

## Tech Stack

Python 3.11+ | SQLite | pandas + pandas_ta | yfinance | dhanhq | APScheduler | Streamlit | matplotlib

## License

Private repository. Not for redistribution.
