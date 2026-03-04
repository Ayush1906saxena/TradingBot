import sqlite3
import os


DB_PATH = "db/market_data.db"

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS daily_ohlcv (
    symbol      TEXT NOT NULL,
    date        TEXT NOT NULL,
    open        REAL NOT NULL,
    high        REAL NOT NULL,
    low         REAL NOT NULL,
    close       REAL NOT NULL,
    volume      INTEGER NOT NULL,
    created_at  TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS minute_ohlcv (
    symbol      TEXT NOT NULL,
    timestamp   TEXT NOT NULL,
    timeframe   TEXT NOT NULL,
    open        REAL NOT NULL,
    high        REAL NOT NULL,
    low         REAL NOT NULL,
    close       REAL NOT NULL,
    volume      INTEGER NOT NULL,
    created_at  TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, timestamp, timeframe)
);

CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       TEXT NOT NULL,
    fill_timestamp  TEXT,
    strategy        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    quantity        INTEGER NOT NULL,
    signal_price    REAL NOT NULL,
    fill_price      REAL,
    order_type      TEXT NOT NULL,
    mode            TEXT NOT NULL,
    broker_order_id TEXT,
    status          TEXT NOT NULL DEFAULT 'PENDING',
    stop_loss       REAL,
    target          REAL,
    pnl             REAL,
    fees            REAL,
    notes           TEXT,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS positions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL,
    strategy            TEXT NOT NULL,
    side                TEXT NOT NULL,
    quantity            INTEGER NOT NULL,
    entry_price         REAL NOT NULL,
    entry_time          TEXT NOT NULL,
    current_price       REAL,
    stop_loss           REAL NOT NULL,
    target              REAL,
    trailing_stop       REAL,
    highest_since_entry REAL,
    unrealized_pnl      REAL,
    status              TEXT NOT NULL DEFAULT 'OPEN',
    closed_at           TEXT,
    close_reason        TEXT,
    UNIQUE(symbol, strategy, status)
);

CREATE TABLE IF NOT EXISTS daily_pnl (
    date            TEXT PRIMARY KEY,
    mode            TEXT NOT NULL,
    total_trades    INTEGER DEFAULT 0,
    winning_trades  INTEGER DEFAULT 0,
    losing_trades   INTEGER DEFAULT 0,
    gross_pnl       REAL DEFAULT 0,
    total_fees      REAL DEFAULT 0,
    net_pnl         REAL DEFAULT 0,
    max_drawdown    REAL DEFAULT 0,
    capital_start   REAL,
    capital_end     REAL
);

CREATE TABLE IF NOT EXISTS system_state (
    key             TEXT PRIMARY KEY,
    value           TEXT NOT NULL,
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS virtual_wallet (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    amount          REAL NOT NULL,
    balance_after   REAL NOT NULL,
    reference_id    INTEGER,
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS virtual_portfolio_snapshots (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp               TEXT NOT NULL,
    cash_balance            REAL NOT NULL,
    positions_value         REAL NOT NULL,
    total_value             REAL NOT NULL,
    unrealized_pnl          REAL NOT NULL,
    realized_pnl_cumulative REAL NOT NULL,
    total_fees_cumulative   REAL NOT NULL,
    day_pnl                 REAL NOT NULL,
    num_open_positions      INTEGER NOT NULL,
    snapshot_reason         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_date ON trades(timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_daily_ohlcv_symbol ON daily_ohlcv(symbol);
CREATE INDEX IF NOT EXISTS idx_minute_ohlcv_symbol_tf ON minute_ohlcv(symbol, timeframe);
"""


def init_db(db_path: str = DB_PATH) -> None:
    """Create database and all tables if they don't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(CREATE_TABLES_SQL)
        conn.commit()
    finally:
        conn.close()


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Return a SQLite connection with row_factory set."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn
