"""Historical simulation engine for backtesting strategies."""
import logging
import math
import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from data.historical import HistoricalDataFetcher
from db import get_connection
from orders.order_manager import calculate_fees
from risk.risk_manager import RiskManager

logger = logging.getLogger(__name__)


class Backtester:
    def __init__(self, config: dict, db_path: str):
        self.config = config
        self.db_path = db_path
        self.bt_config = config["backtest"]
        self.fetcher = HistoricalDataFetcher(db_path, config)
        self._risk_manager = RiskManager(config, db_path, mode="backtest")

    def _load_strategies(self, strategy_name: str = None) -> list:
        from strategies.sma_crossover import SMACrossover
        from strategies.rsi_reversal import RSIReversal
        from strategies.supertrend import SupertrendStrategy
        from strategies.bollinger_bands import BollingerBands
        from strategies.stochastic_oscillator import StochasticOscillator
        from strategies.mean_reversion_zscore import MeanReversionZScore
        from strategies.parabolic_sar import ParabolicSAR
        from strategies.keltner_squeeze import KeltnerSqueeze
        from strategies.rsi_divergence import RSIDivergence
        from strategies.volatility_breakout import VolatilityBreakout
        from strategies.opening_range_breakout import OpeningRangeBreakout
        from strategies.multi_timeframe import MultiTimeframe
        from strategies.ml_ensemble import MLEnsemble
        from strategies.pairs_trading import PairsTrading

        strategy_map = {
            "sma_crossover": SMACrossover,
            "rsi_reversal": RSIReversal,
            "supertrend": SupertrendStrategy,
            "bollinger_bands": BollingerBands,
            "stochastic_oscillator": StochasticOscillator,
            "mean_reversion_zscore": MeanReversionZScore,
            "parabolic_sar": ParabolicSAR,
            "keltner_squeeze": KeltnerSqueeze,
            "rsi_divergence": RSIDivergence,
            "volatility_breakout": VolatilityBreakout,
            "opening_range_breakout": OpeningRangeBreakout,
            "multi_timeframe": MultiTimeframe,
            "ml_ensemble": MLEnsemble,
            "pairs_trading": PairsTrading,
        }

        strategies = []
        for name, cls in strategy_map.items():
            if strategy_name and name != strategy_name:
                continue
            cfg = self.config.get("strategies", {}).get(name, {})
            if not cfg.get("enabled", False):
                continue
            strategies.append(cls(cfg))
        return strategies

    def run(self, strategy_name: str = None) -> dict:
        strategies = self._load_strategies(strategy_name)
        if not strategies:
            logger.warning("No enabled strategies found for backtest")
            return {}

        start = self.bt_config["start_date"]
        end = self.bt_config["end_date"]
        initial_capital = float(self.bt_config["initial_capital"])

        results = {}
        for strategy in strategies:
            logger.info(f"Running backtest: {strategy.name} ({start} to {end})")
            trades = []
            equity_curve = []

            capital = initial_capital

            if strategy.is_multi_symbol:
                # Multi-symbol path: load all symbols, simulate together
                dfs = {}
                for symbol in strategy.symbols:
                    df = self._get_ohlcv(strategy, symbol, start, end)
                    if df is not None and len(df) >= 50:
                        dfs[symbol] = df
                    else:
                        logger.warning(f"Insufficient data for {symbol} — skipping")
                if dfs:
                    trades = self._simulate_multi_symbol(strategy, dfs, capital)
            else:
                for symbol in strategy.symbols:
                    df = self._get_ohlcv(strategy, symbol, start, end)
                    if df is None or len(df) < 50:
                        logger.warning(f"Insufficient data for {symbol} — skipping")
                        continue

                    symbol_trades = self._simulate_symbol(strategy, symbol, df, capital / len(strategy.symbols))
                    trades.extend(symbol_trades)

            metrics = self.calculate_metrics(trades, initial_capital)
            equity_curve = self._build_equity_curve(trades, initial_capital)
            report_path = self.generate_report(strategy.name, trades, metrics, equity_curve)

            results[strategy.name] = {
                "trades": trades,
                "metrics": metrics,
                "equity_curve": equity_curve,
                "report_path": report_path,
            }
            logger.info(
                f"{strategy.name}: Return={metrics['total_return_pct']:.1f}% | "
                f"Trades={metrics['total_trades']} | WinRate={metrics['win_rate_pct']:.1f}%"
            )

        return results

    def _get_ohlcv(self, strategy, symbol: str, start: str, end: str) -> pd.DataFrame | None:
        timeframe = strategy.timeframe
        if timeframe in ("1d",):
            df = self.fetcher.get_daily_df(symbol, start, end)
            if df.empty:
                return None
            df = df.rename(columns={"date": "timestamp"})
        else:
            df = self.fetcher.get_minute_df(symbol, timeframe, start, end)
            if df.empty:
                # Try daily fallback for demo/testing
                df = self.fetcher.get_daily_df(symbol, start, end)
                if df.empty:
                    return None
                df = df.rename(columns={"date": "timestamp"})
        return df

    def _simulate_symbol(self, strategy, symbol: str, df: pd.DataFrame,
                          allocated_capital: float) -> list:
        trades = []
        open_position = None
        slippage_pct = float(self.bt_config.get("slippage_pct", 0.05))

        warmup = max(50, getattr(strategy, 'long_window', 21) + 5)

        for i in range(warmup, len(df)):
            sub_df = df.iloc[:i + 1].copy()
            current_price = float(sub_df.iloc[-1]["close"])

            # Check exit for open position
            if open_position:
                # Update highest_since_entry for trailing stop strategies
                if open_position["side"] == "LONG":
                    open_position["highest_since_entry"] = max(
                        open_position.get("highest_since_entry", open_position["entry_price"]),
                        current_price
                    )
                elif open_position["side"] == "SHORT":
                    open_position["highest_since_entry"] = min(
                        open_position.get("highest_since_entry", open_position["entry_price"]),
                        current_price
                    )

                exit_signal = strategy.should_exit(open_position, current_price, sub_df)
                if exit_signal:
                    # Slippage: BUY costs more, SELL gets less (realistic adverse fill)
                    if exit_signal["action"] == "BUY":
                        fill = current_price * (1 + slippage_pct / 100)
                    else:
                        fill = current_price * (1 - slippage_pct / 100)
                    fees = calculate_fees(fill, open_position["quantity"], exit_signal["action"], "intraday")

                    pnl = (fill - open_position["entry_price"]) * open_position["quantity"]
                    if open_position["side"] == "SHORT":
                        pnl = (open_position["entry_price"] - fill) * open_position["quantity"]
                    pnl -= fees["total"]

                    trades.append({
                        "symbol": symbol, "strategy": strategy.name,
                        "side": exit_signal["action"],
                        "quantity": open_position["quantity"],
                        "entry_price": open_position["entry_price"],
                        "exit_price": fill,
                        "pnl": pnl, "fees": fees["total"],
                        "reason": exit_signal["reason"],
                        "timestamp": str(sub_df.iloc[-1].get("timestamp", i)),
                    })
                    open_position = None
                    continue

            # Generate entry signal
            if not open_position:
                signal = strategy.generate_signal(sub_df, symbol)
                if signal:
                    fill = signal["price"] * (1 + slippage_pct / 100) if signal["action"] == "BUY" \
                        else signal["price"] * (1 - slippage_pct / 100)

                    sl_val = signal.get("stop_loss", fill * 0.98)
                    qty = max(1, int((allocated_capital * 0.02) / abs(fill - sl_val)))
                    qty = min(qty, int(allocated_capital * 0.20 / fill))
                    qty = max(1, qty)

                    open_position = {
                        "symbol": symbol, "strategy": strategy.name,
                        "side": "LONG" if signal["action"] == "BUY" else "SHORT",
                        "quantity": qty,
                        "entry_price": fill,
                        "stop_loss": sl_val,
                        "target": signal.get("target"),
                        "highest_since_entry": fill,
                    }

        # Force close at end
        if open_position:
            fill = float(df.iloc[-1]["close"])
            fees = calculate_fees(fill, open_position["quantity"], "SELL", "intraday")
            pnl = (fill - open_position["entry_price"]) * open_position["quantity"]
            if open_position["side"] == "SHORT":
                pnl = (open_position["entry_price"] - fill) * open_position["quantity"]
            pnl -= fees["total"]
            trades.append({
                "symbol": symbol, "strategy": strategy.name,
                "side": "SELL", "quantity": open_position["quantity"],
                "entry_price": open_position["entry_price"], "exit_price": fill,
                "pnl": pnl, "fees": fees["total"], "reason": "END_OF_DATA",
                "timestamp": str(df.iloc[-1].get("timestamp", len(df))),
            })

        return trades

    def _simulate_multi_symbol(self, strategy, dfs: dict[str, pd.DataFrame],
                                total_capital: float) -> list:
        """Simulate a multi-symbol strategy by iterating aligned dates."""
        trades = []
        open_positions = {}  # symbol -> position dict
        slippage_pct = float(self.bt_config.get("slippage_pct", 0.05))
        allocated_per_symbol = total_capital / max(len(dfs), 1)
        warmup = 60

        # Collect all unique timestamps and sort
        all_timestamps = set()
        for df in dfs.values():
            all_timestamps.update(df["timestamp"].tolist())
        sorted_dates = sorted(all_timestamps)

        for date in sorted_dates[warmup:]:
            # Build sub-DataFrames up to this date
            sub_dfs = {}
            for symbol, df in dfs.items():
                mask = df["timestamp"] <= date
                sub = df[mask]
                if len(sub) >= warmup:
                    sub_dfs[symbol] = sub.copy()

            if not sub_dfs:
                continue

            # Check exits first
            for symbol in list(open_positions.keys()):
                pos = open_positions[symbol]
                if symbol not in sub_dfs:
                    continue
                current_price = float(sub_dfs[symbol].iloc[-1]["close"])

                if pos["side"] == "LONG":
                    pos["highest_since_entry"] = max(
                        pos.get("highest_since_entry", pos["entry_price"]), current_price)
                else:
                    pos["highest_since_entry"] = min(
                        pos.get("highest_since_entry", pos["entry_price"]), current_price)

                exit_signal = strategy.should_exit(pos, current_price, sub_dfs[symbol])
                if exit_signal:
                    if exit_signal["action"] == "BUY":
                        fill = current_price * (1 + slippage_pct / 100)
                    else:
                        fill = current_price * (1 - slippage_pct / 100)
                    fees = calculate_fees(fill, pos["quantity"], exit_signal["action"], "intraday")
                    pnl = (fill - pos["entry_price"]) * pos["quantity"]
                    if pos["side"] == "SHORT":
                        pnl = (pos["entry_price"] - fill) * pos["quantity"]
                    pnl -= fees["total"]

                    trades.append({
                        "symbol": symbol, "strategy": strategy.name,
                        "side": exit_signal["action"], "quantity": pos["quantity"],
                        "entry_price": pos["entry_price"], "exit_price": fill,
                        "pnl": pnl, "fees": fees["total"],
                        "reason": exit_signal["reason"], "timestamp": str(date),
                    })
                    del open_positions[symbol]

            # Generate new entry signals
            signals = strategy.generate_signal_multi(sub_dfs)
            for signal in signals:
                sym = signal["symbol"]
                if sym in open_positions:
                    continue

                fill = signal["price"] * (1 + slippage_pct / 100) if signal["action"] == "BUY" \
                    else signal["price"] * (1 - slippage_pct / 100)

                sl_val = signal.get("stop_loss", fill * 0.98)
                qty = max(1, int((allocated_per_symbol * 0.02) / max(abs(fill - sl_val), 0.01)))
                qty = min(qty, int(allocated_per_symbol * 0.20 / max(fill, 0.01)))
                qty = max(1, qty)

                open_positions[sym] = {
                    "symbol": sym, "strategy": strategy.name,
                    "side": "LONG" if signal["action"] == "BUY" else "SHORT",
                    "quantity": qty, "entry_price": fill,
                    "stop_loss": sl_val, "target": signal.get("target"),
                    "highest_since_entry": fill,
                }

        # Force close remaining
        for symbol, pos in open_positions.items():
            if symbol in dfs:
                fill = float(dfs[symbol].iloc[-1]["close"])
                fees = calculate_fees(fill, pos["quantity"], "SELL", "intraday")
                pnl = (fill - pos["entry_price"]) * pos["quantity"]
                if pos["side"] == "SHORT":
                    pnl = (pos["entry_price"] - fill) * pos["quantity"]
                pnl -= fees["total"]
                trades.append({
                    "symbol": symbol, "strategy": strategy.name,
                    "side": "SELL", "quantity": pos["quantity"],
                    "entry_price": pos["entry_price"], "exit_price": fill,
                    "pnl": pnl, "fees": fees["total"], "reason": "END_OF_DATA",
                    "timestamp": str(dfs[symbol].iloc[-1].get("timestamp", "")),
                })

        return trades

    def calculate_metrics(self, trades: list, initial_capital: float) -> dict:
        if not trades:
            return {k: 0 for k in [
                "total_return_pct", "total_return_inr", "cagr_pct", "max_drawdown_pct",
                "max_drawdown_inr", "sharpe_ratio", "sortino_ratio", "win_rate_pct",
                "avg_win_inr", "avg_loss_inr", "profit_factor", "total_trades",
                "total_fees_inr", "best_trade_inr", "worst_trade_inr",
                "avg_holding_period", "expectancy_inr"
            ]}

        pnls = [t["pnl"] for t in trades]
        total_pnl = sum(pnls)
        total_fees = sum(t.get("fees", 0) for t in trades)
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        win_rate = len(wins) / len(pnls) * 100 if pnls else 0
        avg_win = sum(wins) / len(wins) if wins else 0
        avg_loss = sum(losses) / len(losses) if losses else 0
        profit_factor = abs(sum(wins) / sum(losses)) if losses and sum(losses) != 0 else float("inf")

        # Equity curve for drawdown and Sharpe
        equity = [initial_capital]
        for p in pnls:
            equity.append(equity[-1] + p)

        peak = initial_capital
        max_dd = 0
        for e in equity:
            if e > peak:
                peak = e
            dd = (peak - e) / peak * 100
            max_dd = max(max_dd, dd)

        max_dd_inr = max(0, max([p - e for p, e in zip([initial_capital] + equity[:-1], equity)], default=0))

        # Daily returns approximation (equity[i] is value BEFORE trade i applied)
        returns = [pnls[i] / equity[i] for i in range(len(pnls))] if len(equity) > 1 else []
        rf_daily = 0.06 / 252
        excess = [r - rf_daily for r in returns]

        if excess and len(excess) > 1:
            std_excess = float(np.std(excess, ddof=1))
            sharpe = (float(np.mean(excess)) / std_excess * math.sqrt(252)) if std_excess > 1e-10 else 0
        else:
            sharpe = 0

        neg_excess = [r for r in excess if r < 0]
        if neg_excess and len(neg_excess) > 1:
            std_neg = float(np.std(neg_excess, ddof=1))
            sortino = (float(np.mean(excess)) / std_neg * math.sqrt(252)) if std_neg > 1e-10 else 0
        else:
            sortino = 0

        final_value = equity[-1]
        total_return_pct = (final_value - initial_capital) / initial_capital * 100
        expectancy = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)

        return {
            "total_return_pct": round(total_return_pct, 2),
            "total_return_inr": round(total_pnl, 2),
            "cagr_pct": round(total_return_pct, 2),  # Simplified
            "max_drawdown_pct": round(max_dd, 2),
            "max_drawdown_inr": round(max_dd_inr, 2),
            "sharpe_ratio": round(sharpe, 2),
            "sortino_ratio": round(sortino, 2),
            "win_rate_pct": round(win_rate, 1),
            "avg_win_inr": round(avg_win, 2),
            "avg_loss_inr": round(avg_loss, 2),
            "profit_factor": round(profit_factor, 2),
            "total_trades": len(trades),
            "total_fees_inr": round(total_fees, 2),
            "best_trade_inr": round(max(pnls), 2) if pnls else 0,
            "worst_trade_inr": round(min(pnls), 2) if pnls else 0,
            "avg_holding_period": 1,
            "expectancy_inr": round(expectancy, 2),
        }

    def _build_equity_curve(self, trades: list, initial_capital: float) -> list:
        equity = initial_capital
        curve = [{"timestamp": "start", "equity": initial_capital}]
        for t in trades:
            equity += t["pnl"]
            curve.append({
                "timestamp": t.get("timestamp", ""),
                "equity": round(equity, 2),
                "symbol": t["symbol"],
                "pnl": t["pnl"],
            })
        return curve

    def generate_report(self, strategy_name: str, trades: list,
                        metrics: dict, equity_curve: list) -> str:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        os.makedirs("logs", exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f"logs/backtest_{strategy_name}_{ts}.html"

        equities = [e["equity"] for e in equity_curve]
        fig, axes = plt.subplots(2, 1, figsize=(12, 8))

        axes[0].plot(equities, color="green" if equities[-1] > equities[0] else "red")
        axes[0].set_title(f"{strategy_name} — Equity Curve")
        axes[0].set_ylabel("Portfolio Value (₹)")
        axes[0].grid(True, alpha=0.3)

        # Drawdown
        peak = equities[0]
        drawdowns = []
        for e in equities:
            peak = max(peak, e)
            drawdowns.append(-(peak - e) / peak * 100)
        axes[1].fill_between(range(len(drawdowns)), drawdowns, color="red", alpha=0.4)
        axes[1].set_title("Drawdown (%)")
        axes[1].set_ylabel("Drawdown (%)")
        axes[1].grid(True, alpha=0.3)

        plt.tight_layout()
        img_path = report_path.replace(".html", ".png")
        plt.savefig(img_path, dpi=100)
        plt.close()

        m = metrics
        html = f"""<!DOCTYPE html>
<html>
<head><title>Backtest Report — {strategy_name}</title>
<style>body{{font-family:monospace;margin:20px;}}
table{{border-collapse:collapse;}} td,th{{border:1px solid #ccc;padding:6px 12px;}}
th{{background:#f0f0f0;}} .pos{{color:green;}} .neg{{color:red;}}</style>
</head><body>
<h1>Backtest Report: {strategy_name}</h1>
<h3>Performance Metrics</h3>
<table>
<tr><th>Metric</th><th>Value</th></tr>
<tr><td>Total Return</td><td class="{'pos' if m['total_return_pct']>0 else 'neg'}">{m['total_return_pct']:.2f}% (₹{m['total_return_inr']:,.0f})</td></tr>
<tr><td>Win Rate</td><td>{m['win_rate_pct']:.1f}%</td></tr>
<tr><td>Max Drawdown</td><td class="neg">{m['max_drawdown_pct']:.2f}%</td></tr>
<tr><td>Sharpe Ratio</td><td>{m['sharpe_ratio']:.2f}</td></tr>
<tr><td>Sortino Ratio</td><td>{m['sortino_ratio']:.2f}</td></tr>
<tr><td>Profit Factor</td><td>{m['profit_factor']:.2f}</td></tr>
<tr><td>Total Trades</td><td>{m['total_trades']}</td></tr>
<tr><td>Avg Win</td><td class="pos">₹{m['avg_win_inr']:,.2f}</td></tr>
<tr><td>Avg Loss</td><td class="neg">₹{m['avg_loss_inr']:,.2f}</td></tr>
<tr><td>Best Trade</td><td class="pos">₹{m['best_trade_inr']:,.2f}</td></tr>
<tr><td>Worst Trade</td><td class="neg">₹{m['worst_trade_inr']:,.2f}</td></tr>
<tr><td>Total Fees</td><td>₹{m['total_fees_inr']:,.2f}</td></tr>
<tr><td>Expectancy</td><td>₹{m['expectancy_inr']:,.2f}</td></tr>
</table>
<br>
<img src="{os.path.basename(img_path)}" style="max-width:100%">
<h3>Trades ({len(trades)})</h3>
<table>
<tr><th>#</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Entry</th><th>Exit</th><th>P&L</th><th>Fees</th><th>Reason</th></tr>
"""
        for i, t in enumerate(trades, 1):
            cls = "pos" if t["pnl"] > 0 else "neg"
            html += (
                f"<tr><td>{i}</td><td>{t['symbol']}</td><td>{t['side']}</td>"
                f"<td>{t['quantity']}</td><td>₹{t['entry_price']:,.2f}</td>"
                f"<td>₹{t['exit_price']:,.2f}</td>"
                f"<td class='{cls}'>₹{t['pnl']:+,.2f}</td>"
                f"<td>₹{t.get('fees',0):,.2f}</td><td>{t.get('reason','')}</td></tr>\n"
            )
        html += "</table></body></html>"

        with open(report_path, "w") as f:
            f.write(html)

        return report_path

    def walk_forward_validation(self, strategy_name: str) -> dict:
        cfg = self.bt_config
        train_days = cfg.get("walk_forward_train_days", 250)
        test_days = cfg.get("walk_forward_test_days", 50)
        n_splits = cfg.get("walk_forward_splits", 5)

        end = datetime.strptime(cfg["end_date"], "%Y-%m-%d")
        splits = []
        profitable_splits = 0

        for i in range(n_splits):
            test_end = end - timedelta(days=i * test_days)
            test_start = test_end - timedelta(days=test_days)
            train_end = test_start - timedelta(days=1)
            train_start = train_end - timedelta(days=train_days)

            # Run on test period only (simplified WFV)
            test_config = dict(self.config)
            test_config["backtest"] = dict(self.bt_config)
            test_config["backtest"]["start_date"] = test_start.strftime("%Y-%m-%d")
            test_config["backtest"]["end_date"] = test_end.strftime("%Y-%m-%d")

            test_bt = Backtester(test_config, self.db_path)
            test_results = test_bt.run(strategy_name)

            split_metrics = {}
            if strategy_name in test_results:
                split_metrics = test_results[strategy_name]["metrics"]
                if split_metrics.get("total_return_pct", 0) > 0:
                    profitable_splits += 1

            splits.append({
                "split": i + 1,
                "train": f"{train_start.date()} to {train_end.date()}",
                "test": f"{test_start.date()} to {test_end.date()}",
                "metrics": split_metrics,
            })

        return {
            "splits": splits,
            "profitable_splits": profitable_splits,
            "total_splits": n_splits,
            "robust": profitable_splits >= 4,
        }
