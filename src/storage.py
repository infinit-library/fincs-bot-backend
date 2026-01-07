import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


DB_PATH = Path(__file__).resolve().parent.parent / "data" / "fincs.db"


def utcnow() -> str:
    """Return ISO 8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def sha256_text(text: str) -> str:
    """Hash helper used for dedupe."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def connect_db(db_path: str | Path = DB_PATH) -> sqlite3.Connection:
    """Open SQLite connection and ensure schema exists."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    """Create tables and backfill new columns when upgrading."""
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at TEXT NOT NULL,
            channel TEXT,
            raw_hash TEXT UNIQUE NOT NULL,
            raw_text TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_raw_scraped_at ON raw_snapshots(scraped_at DESC);")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS parsed_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraped_at TEXT NOT NULL,
            segment_hash TEXT UNIQUE NOT NULL,
            segment_text TEXT NOT NULL,
            is_trading INTEGER NOT NULL DEFAULT 0,
            pair TEXT,
            action TEXT,
            side TEXT,
            lot_ratio REAL,
            is_add INTEGER NOT NULL DEFAULT 0,
            entry_price REAL,
            sl_price REAL,
            tp_price REAL,
            signal_id TEXT,
            created_at TEXT NOT NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_parsed_trading ON parsed_events(is_trading, scraped_at DESC);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_parsed_pair ON parsed_events(pair, scraped_at DESC);")

    # Backfill columns if the table already existed
    existing_cols = {row[1] for row in cur.execute("PRAGMA table_info(parsed_events)").fetchall()}
    for col_def in [
        ("entry_price", "REAL"),
        ("sl_price", "REAL"),
        ("tp_price", "REAL"),
        ("signal_id", "TEXT"),
    ]:
        col_name, col_type = col_def
        if col_name not in existing_cols:
            cur.execute(f"ALTER TABLE parsed_events ADD COLUMN {col_name} {col_type};")

    # Track executions to avoid double-firing broker orders
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS executed_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            segment_hash TEXT NOT NULL,
            broker TEXT NOT NULL,
            status TEXT NOT NULL,
            order_id TEXT,
            error_message TEXT,
            payload TEXT,
            created_at TEXT NOT NULL
        );
        """
    )
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_executed_hash_broker ON executed_orders(segment_hash, broker);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_executed_created ON executed_orders(created_at DESC);")

    conn.commit()


def insert_raw_snapshot(
    conn: sqlite3.Connection,
    scraped_at: str,
    channel: str,
    raw_hash: str,
    raw_text: str,
) -> bool:
    """Insert raw snapshot; return True if inserted, False if duplicate."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO raw_snapshots (scraped_at, channel, raw_hash, raw_text, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (scraped_at, channel, raw_hash, raw_text, utcnow()),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def insert_parsed_event(
    conn: sqlite3.Connection,
    scraped_at: str,
    segment_hash: str,
    segment_text: str,
    is_trading: bool,
    pair: Optional[str],
    action: Optional[str],
    side: Optional[str],
    lot_ratio: Optional[float],
    is_add: bool,
    entry_price: Optional[float],
    sl_price: Optional[float],
    tp_price: Optional[float],
    signal_id: Optional[str],
) -> bool:
    """Insert parsed segment; return True if inserted, False if duplicate."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO parsed_events (
                scraped_at, segment_hash, segment_text, is_trading,
                pair, action, side, lot_ratio, is_add,
                entry_price, sl_price, tp_price, signal_id,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scraped_at,
                segment_hash,
                segment_text,
                1 if is_trading else 0,
                pair,
                action,
                side,
                lot_ratio,
                1 if is_add else 0,
                entry_price,
                sl_price,
                tp_price,
                signal_id,
                utcnow(),
            ),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def _rows_to_dicts(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]


def get_all_trading_events(conn: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM parsed_events
        WHERE is_trading = 1
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    )
    return _rows_to_dicts(cur.fetchall())


def get_events_by_pair(conn: sqlite3.Connection, pair: str, limit: int = 100) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM parsed_events
        WHERE is_trading = 1 AND pair = ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (pair, limit),
    )
    return _rows_to_dicts(cur.fetchall())


def get_latest_trading_event(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM parsed_events
        WHERE is_trading = 1
        ORDER BY id DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    return dict(row) if row else None


def get_latest_snapshot(conn: sqlite3.Connection) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM raw_snapshots
        ORDER BY datetime(scraped_at) DESC
        LIMIT 1
        """
    )
    row = cur.fetchone()
    return dict(row) if row else None


def get_recent_raw(conn: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM raw_snapshots
        ORDER BY datetime(scraped_at) DESC
        LIMIT ?
        """,
        (limit,),
    )
    return _rows_to_dicts(cur.fetchall())


def get_event_statistics(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS c FROM parsed_events")
    total_events = cur.fetchone()["c"]

    cur.execute("SELECT COUNT(*) AS c FROM parsed_events WHERE is_trading = 1")
    trading_events = cur.fetchone()["c"]

    cur.execute("SELECT COUNT(*) AS c FROM parsed_events WHERE is_trading = 0")
    non_trading = cur.fetchone()["c"]

    cur.execute(
        "SELECT pair, COUNT(*) AS c FROM parsed_events WHERE is_trading = 1 AND pair IS NOT NULL GROUP BY pair"
    )
    by_pair = {row["pair"]: row["c"] for row in cur.fetchall()}

    cur.execute(
        "SELECT action, COUNT(*) AS c FROM parsed_events WHERE is_trading = 1 AND action IS NOT NULL GROUP BY action"
    )
    by_action = {row["action"]: row["c"] for row in cur.fetchall()}

    return {
        "total_events": total_events,
        "trading_events": trading_events,
        "non_trading_events": non_trading,
        "by_pair": by_pair,
        "by_action": by_action,
    }


# --- Execution tracking helpers ------------------------------------------------


def record_execution(
    conn: sqlite3.Connection,
    segment_hash: str,
    broker: str,
    status: str,
    order_id: Optional[str] = None,
    error_message: Optional[str] = None,
    payload: Optional[str] = None,
) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR REPLACE INTO executed_orders
        (segment_hash, broker, status, order_id, error_message, payload, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (segment_hash, broker, status, order_id, error_message, payload, utcnow()),
    )
    conn.commit()


def was_executed(conn: sqlite3.Connection, segment_hash: str, broker: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM executed_orders WHERE segment_hash = ? AND broker = ? LIMIT 1",
        (segment_hash, broker),
    )
    return cur.fetchone() is not None


def list_executions(conn: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT * FROM executed_orders
        ORDER BY datetime(created_at) DESC
        LIMIT ?
        """,
        (limit,),
    )
    return _rows_to_dicts(cur.fetchall())
