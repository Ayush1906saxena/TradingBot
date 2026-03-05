"""Streamlit Dashboard — 5-page trading system UI."""
import os
import sqlite3
import sys
from datetime import date, datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import yaml

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import DB_PATH, get_connection

st.set_page_config(
    page_title="Algo Trading Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Auto-refresh every 30 seconds
st.markdown(
    '<meta http-equiv="refresh" content="30">',
    unsafe_allow_html=True
)


def load_config():
    try:
        with open("configs/config.yaml") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}


def get_db():
    return get_connection(DB_PATH)


def get_open_positions():
    try:
        conn = get_db()
        rows = conn.execute("SELECT * FROM positions WHERE status='OPEN' ORDER BY entry_time DESC").fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_trades(mode=None, limit=500):
    try:
        conn = get_db()
        q = "SELECT * FROM trades WHERE status='FILLED'"
        params = []
        if mode:
            q += " AND mode=?"
            params.append(mode)
        q += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(q, params).fetchall()
        conn.close()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def get_virtual_summary():
    try:
        conn = get_db()
        last = conn.execute(
            "SELECT * FROM virtual_portfolio_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
        initial = conn.execute(
            "SELECT balance_after FROM virtual_wallet WHERE event_type='INITIAL_DEPOSIT' "
            "OR event_type='RESET' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if last:
            d = dict(last)
            d["initial_capital"] = dict(initial)["balance_after"] if initial else 100000
            return d
    except Exception:
        pass
    return None


def get_equity_curve():
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT timestamp, total_value FROM virtual_portfolio_snapshots ORDER BY id ASC"
        ).fetchall()
        conn.close()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def get_wallet_history(limit=20):
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT * FROM virtual_wallet ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


def get_system_state():
    try:
        conn = get_db()
        rows = conn.execute("SELECT key, value FROM system_state").fetchall()
        conn.close()
        return {r["key"]: r["value"] for r in rows}
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────
# Sidebar Navigation
# ─────────────────────────────────────────────────────────
config = load_config()
mode = config.get("mode", "backtest").upper()

st.sidebar.title("📈 Algo Trading")
st.sidebar.markdown(f"**Mode:** `{mode}`")
if mode == "PAPER":
    st.sidebar.warning("🎭 DUMMY MONEY")
elif mode == "LIVE":
    st.sidebar.error("⚠️ REAL MONEY")

page = st.sidebar.radio(
    "Navigate",
    ["Live Overview", "Trade History", "Performance Analytics",
     "Backtest Runner", "Settings & Control"]
)

st.sidebar.markdown("---")
st.sidebar.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")

# ─────────────────────────────────────────────────────────
# PAGE 1 — Live Overview
# ─────────────────────────────────────────────────────────
if page == "Live Overview":
    st.title("📊 Live Overview")

    # Mode banner
    if mode == "PAPER":
        st.info("🎭 **PAPER TRADING MODE** — Using dummy money. No real orders placed.")
    elif mode == "LIVE":
        st.error("⚠️ **LIVE TRADING MODE** — Real money at stake.")

    # Virtual wallet summary (paper mode)
    if mode == "PAPER":
        vp = get_virtual_summary()
        if vp:
            initial = vp.get("initial_capital", 100000)
            total = vp.get("total_value", initial)
            cash = vp.get("cash_balance", initial)
            pos_val = vp.get("positions_value", 0)
            day_pnl = vp.get("day_pnl", 0)
            total_return_pct = (total - initial) / initial * 100 if initial else 0

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("💰 Virtual Cash", f"₹{cash:,.0f}")
            col2.metric("📦 Positions Value", f"₹{pos_val:,.0f}")
            col3.metric("🏦 Total Portfolio", f"₹{total:,.0f}",
                        delta=f"{total_return_pct:+.2f}% since start")
            col4.metric("📅 Today's P&L", f"₹{day_pnl:+,.0f}",
                        delta_color="normal" if day_pnl >= 0 else "inverse")
        else:
            st.info("No virtual portfolio data yet. Start paper trading first.")

    # Open Positions
    st.subheader("Open Positions")
    positions = get_open_positions()
    if positions:
        pos_df = pd.DataFrame(positions)
        cols = ["symbol", "strategy", "side", "quantity", "entry_price",
                "current_price", "stop_loss", "target", "unrealized_pnl", "entry_time"]
        available_cols = [c for c in cols if c in pos_df.columns]
        st.dataframe(pos_df[available_cols], use_container_width=True)
    else:
        st.info("No open positions")

    # Recent trades
    st.subheader("Last 10 Signals")
    trades = get_trades(limit=10)
    if not trades.empty:
        st.dataframe(trades[["timestamp", "symbol", "strategy", "side", "quantity",
                              "fill_price", "status", "pnl", "mode"]
                             ].head(10), use_container_width=True)
    else:
        st.info("No trades yet")

    # System state
    sys_state = get_system_state()
    if sys_state:
        st.subheader("System Status")
        col1, col2, col3 = st.columns(3)
        ks = sys_state.get("kill_switch_active", "0")
        col1.metric("Kill Switch", "🔴 ACTIVE" if ks == "1" else "🟢 OFF")
        col2.metric("Consecutive Losses", sys_state.get("consecutive_losses", "0"))
        col3.metric("Daily Loss Total", f"₹{float(sys_state.get('daily_loss_total', 0)):,.0f}")

# ─────────────────────────────────────────────────────────
# PAGE 2 — Trade History
# ─────────────────────────────────────────────────────────
elif page == "Trade History":
    st.title("📋 Trade History")

    col1, col2, col3 = st.columns(3)
    with col1:
        filter_mode = st.selectbox("Mode", ["All", "paper", "live", "backtest"])
    with col2:
        filter_strategy = st.selectbox(
            "Strategy",
            ["All", "sma_crossover", "rsi_reversal", "supertrend", "bollinger_bands",
             "stochastic_oscillator", "mean_reversion_zscore", "parabolic_sar",
             "keltner_squeeze", "rsi_divergence", "volatility_breakout",
             "opening_range_breakout", "multi_timeframe", "ml_ensemble", "pairs_trading"]
        )
    with col3:
        filter_outcome = st.selectbox("Outcome", ["All", "Win", "Loss"])

    trades = get_trades(mode=None if filter_mode == "All" else filter_mode, limit=1000)

    if not trades.empty:
        if filter_strategy != "All":
            trades = trades[trades["strategy"] == filter_strategy]
        if filter_outcome == "Win":
            trades = trades[trades["pnl"] > 0]
        elif filter_outcome == "Loss":
            trades = trades[trades["pnl"] <= 0]

        # Show paper badge
        if "mode" in trades.columns:
            trades["mode_badge"] = trades["mode"].apply(
                lambda m: "📊 PAPER" if m == "paper" else "💰 LIVE" if m == "live" else "🔄 BT"
            )

        st.dataframe(trades, use_container_width=True, height=400)

        # Download button
        csv = trades.to_csv(index=False)
        st.download_button("⬇️ Download CSV", csv, "trades.csv", "text/csv")

        # Quick stats
        if "pnl" in trades.columns:
            pnls = trades["pnl"].dropna()
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Trades", len(trades))
            col2.metric("Win Rate", f"{(pnls > 0).mean() * 100:.1f}%")
            col3.metric("Net P&L", f"₹{pnls.sum():+,.0f}")
            col4.metric("Avg Trade", f"₹{pnls.mean():+,.0f}")
    else:
        st.info("No trades found")

# ─────────────────────────────────────────────────────────
# PAGE 3 — Performance Analytics
# ─────────────────────────────────────────────────────────
elif page == "Performance Analytics":
    st.title("📈 Performance Analytics")

    # Equity curve (paper mode)
    st.subheader("Equity Curve")
    equity_df = get_equity_curve()
    if not equity_df.empty:
        equity_df["timestamp"] = pd.to_datetime(equity_df["timestamp"])
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=equity_df["timestamp"], y=equity_df["total_value"],
            fill="tozeroy", line=dict(color="green", width=2),
            name="Portfolio Value"
        ))
        fig.update_layout(title="Virtual Portfolio Equity Curve",
                          xaxis_title="Time", yaxis_title="Value (₹)",
                          height=400)
        st.plotly_chart(fig, use_container_width=True)

        # Drawdown
        peak = equity_df["total_value"].cummax()
        drawdown = (equity_df["total_value"] - peak) / peak * 100
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=equity_df["timestamp"], y=drawdown,
            fill="tozeroy", line=dict(color="red", width=1),
            name="Drawdown %"
        ))
        fig2.update_layout(title="Drawdown", yaxis_title="Drawdown (%)", height=250)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No equity curve data. Run in paper mode for 5+ minutes.")

    # Strategy comparison
    st.subheader("Strategy Comparison")
    trades = get_trades(limit=10000)
    if not trades.empty and "pnl" in trades.columns:
        by_strategy = trades.groupby("strategy").agg(
            total_trades=("id", "count"),
            net_pnl=("pnl", "sum"),
            win_rate=("pnl", lambda x: (x > 0).mean() * 100),
            avg_pnl=("pnl", "mean"),
            total_fees=("fees", "sum"),
        ).reset_index()
        st.dataframe(by_strategy.round(2), use_container_width=True)

        # P&L by symbol
        by_symbol = trades.groupby("symbol")["pnl"].sum().reset_index()
        by_symbol.columns = ["symbol", "net_pnl"]
        fig3 = px.bar(by_symbol.sort_values("net_pnl"), x="symbol", y="net_pnl",
                      title="Net P&L by Symbol", color="net_pnl",
                      color_continuous_scale=["red", "green"])
        st.plotly_chart(fig3, use_container_width=True)

        # Monthly returns (if enough data)
        if "timestamp" in trades.columns:
            trades["timestamp"] = pd.to_datetime(trades["timestamp"])
            trades["month"] = trades["timestamp"].dt.to_period("M").astype(str)
            monthly = trades.groupby("month")["pnl"].sum().reset_index()
            if len(monthly) > 1:
                fig4 = px.bar(monthly, x="month", y="pnl",
                              title="Monthly P&L",
                              color="pnl", color_continuous_scale=["red", "green"])
                st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("No trade data available for analysis")

# ─────────────────────────────────────────────────────────
# PAGE 4 — Backtest Runner
# ─────────────────────────────────────────────────────────
elif page == "Backtest Runner":
    st.title("🔄 Backtest Runner")

    col1, col2 = st.columns(2)
    with col1:
        strategy_choice = st.selectbox(
            "Strategy",
            ["All Enabled", "sma_crossover", "rsi_reversal", "supertrend", "bollinger_bands",
             "stochastic_oscillator", "mean_reversion_zscore", "parabolic_sar",
             "keltner_squeeze", "rsi_divergence", "volatility_breakout",
             "opening_range_breakout", "multi_timeframe", "ml_ensemble", "pairs_trading"]
        )
        start_date = st.date_input("Start Date", value=date(2024, 1, 1))
    with col2:
        symbols_input = st.text_input("Symbols (comma-separated)", "RELIANCE,TCS,HDFCBANK,INFY,ICICIBANK")
        end_date = st.date_input("End Date", value=date(2025, 12, 31))

    if st.button("🚀 Run Backtest", type="primary"):
        with st.spinner("Running backtest..."):
            try:
                import yaml
                from engine.backtester import Backtester

                bt_config = load_config()
                bt_config["backtest"]["start_date"] = str(start_date)
                bt_config["backtest"]["end_date"] = str(end_date)

                symbols = [s.strip() for s in symbols_input.split(",")]
                for s_name in bt_config.get("strategies", {}):
                    if bt_config["strategies"][s_name].get("enabled"):
                        bt_config["strategies"][s_name]["symbols"] = symbols

                backtester = Backtester(bt_config, DB_PATH)
                strategy_name = None if strategy_choice == "All Enabled" else strategy_choice
                results = backtester.run(strategy_name)

                for name, result in results.items():
                    metrics = result["metrics"]
                    st.success(f"✅ {name} completed")

                    m_cols = st.columns(4)
                    m_cols[0].metric("Return", f"{metrics.get('total_return_pct', 0):.1f}%")
                    m_cols[1].metric("Win Rate", f"{metrics.get('win_rate_pct', 0):.1f}%")
                    m_cols[2].metric("Max Drawdown", f"{metrics.get('max_drawdown_pct', 0):.1f}%")
                    m_cols[3].metric("Sharpe", f"{metrics.get('sharpe_ratio', 0):.2f}")

                    # Equity curve
                    ec = result.get("equity_curve", [])
                    if ec:
                        ec_df = pd.DataFrame(ec)
                        fig = px.line(ec_df, y="equity", title=f"{name} — Equity Curve")
                        st.plotly_chart(fig, use_container_width=True)

                    if result.get("report_path"):
                        st.info(f"HTML report saved: {result['report_path']}")

            except Exception as e:
                st.error(f"Backtest failed: {e}")
                import traceback
                st.code(traceback.format_exc())

# ─────────────────────────────────────────────────────────
# PAGE 5 — Settings & Control
# ─────────────────────────────────────────────────────────
elif page == "Settings & Control":
    st.title("⚙️ Settings & Control")

    config = load_config()

    # Current config
    with st.expander("📄 Current Configuration (read-only)"):
        st.code(yaml.dump(config, default_flow_style=False, allow_unicode=True), language="yaml")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🔴 Emergency Controls")

        sys_state = get_system_state()
        ks_active = sys_state.get("kill_switch_active", "0") == "1"

        if ks_active:
            st.error("🚨 KILL SWITCH IS ACTIVE — Trading halted")
            if st.button("🔓 Reset Kill Switch", type="primary"):
                try:
                    conn = get_db()
                    conn.execute(
                        "INSERT OR REPLACE INTO system_state (key, value) "
                        "VALUES ('kill_switch_active', '0')"
                    )
                    conn.execute(
                        "INSERT OR REPLACE INTO system_state (key, value) "
                        "VALUES ('consecutive_losses', '0')"
                    )
                    conn.commit()
                    conn.close()
                    st.success("Kill switch reset!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed: {e}")
        else:
            st.success("🟢 Kill switch is OFF — System normal")
            confirm = st.checkbox("Confirm: I want to HALT all trading")
            if confirm and st.button("🔴 Activate KILL SWITCH", type="secondary"):
                try:
                    conn = get_db()
                    conn.execute(
                        "INSERT OR REPLACE INTO system_state (key, value) "
                        "VALUES ('kill_switch_active', '1')"
                    )
                    conn.commit()
                    conn.close()
                    st.warning("Kill switch activated!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed: {e}")

    with col2:
        st.subheader("Strategy Toggles")
        for s_name, s_cfg in config.get("strategies", {}).items():
            enabled = s_cfg.get("enabled", False)
            st.write(f"{'✅' if enabled else '❌'} **{s_name}** — "
                     f"{s_cfg.get('timeframe', '15min')} | "
                     f"Symbols: {', '.join(s_cfg.get('symbols', []))}")

    # Paper mode controls
    if config.get("mode") == "paper":
        st.subheader("🎭 Paper Mode Controls")
        vp_summary = get_virtual_summary()

        if vp_summary:
            col1, col2, col3 = st.columns(3)
            col1.metric("Cash Balance", f"₹{vp_summary.get('cash_balance', 0):,.0f}")
            col2.metric("Positions Value", f"₹{vp_summary.get('positions_value', 0):,.0f}")
            col3.metric("Total Value", f"₹{vp_summary.get('total_value', 0):,.0f}")

        new_capital = st.number_input(
            "New Virtual Capital Amount (₹)",
            min_value=10000, max_value=10000000,
            value=100000, step=10000
        )
        confirm_reset = st.checkbox("Confirm: Reset virtual wallet (this clears all paper trade history)")
        if confirm_reset and st.button("🔄 Reset Virtual Wallet", type="secondary"):
            try:
                conn = get_db()
                from datetime import datetime
                conn.execute(
                    "INSERT INTO virtual_wallet (timestamp, event_type, amount, balance_after, notes) "
                    "VALUES (?, 'RESET', ?, ?, 'Dashboard reset')",
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), new_capital, new_capital)
                )
                conn.commit()
                conn.close()
                st.success(f"Virtual wallet reset to ₹{new_capital:,.0f}")
                st.rerun()
            except Exception as e:
                st.error(f"Reset failed: {e}")

        # Wallet transaction ledger
        st.subheader("Virtual Wallet Ledger (Last 20 entries)")
        wallet_df = get_wallet_history(20)
        if not wallet_df.empty:
            st.dataframe(wallet_df, use_container_width=True)
        else:
            st.info("No wallet transactions yet")

    # System status
    st.subheader("System Status")
    sys_state = get_system_state()
    if sys_state:
        status_df = pd.DataFrame(list(sys_state.items()), columns=["Key", "Value"])
        st.dataframe(status_df, use_container_width=True)
