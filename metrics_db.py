# metrics_db.py
from pathlib import Path
from datetime import datetime, timedelta, timezone
import sqlite3
from typing import Optional, Dict

DB_PATH = Path("python/data/metrics.sqlite")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

_SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS series(
  key      TEXT PRIMARY KEY,     -- "metric:source:item"
  kind     TEXT NOT NULL,        -- price | daily_sales | market_listings | avg_unbox_usd | unbox_roi_pct | playing
  source   TEXT NOT NULL,        -- nazwa pliku/źródła bez .ndjson (np. steam_prices)
  item_id  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ticks(
  key     TEXT NOT NULL,
  ts_utc  INTEGER NOT NULL,
  value   REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_ticks_key_ts ON ticks(key, ts_utc);

CREATE TABLE IF NOT EXISTS kv(
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
"""

def open_db() -> sqlite3.Connection:
    cn = sqlite3.connect(DB_PATH)
    cn.row_factory = sqlite3.Row
    for stmt in _SCHEMA.strip().split(";"):
        s = stmt.strip()
        if s:
            cn.execute(s)
    cn.commit()
    return cn

def upsert_series(cn, key, kind, source, item_id):
    cn.execute(
        "INSERT INTO series(key,kind,source,item_id) VALUES(?,?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET kind=excluded.kind, source=excluded.source, item_id=excluded.item_id",
        (key, kind, source, item_id),
    )
    cn.commit()

def add_tick(cn, key, ts_utc, value):
    cn.execute("INSERT OR IGNORE INTO ticks(key, ts_utc, value) VALUES(?,?,?)", (key, ts_utc, float(value)))
    cn.commit()

def latest_value(cn, key) -> Optional[float]:
    r = cn.execute("SELECT value FROM ticks WHERE key=? ORDER BY ts_utc DESC LIMIT 1", (key,)).fetchone()
    return None if r is None else float(r[0])

def value_at_or_before(cn, key, cutoff) -> Optional[float]:
    r = cn.execute(
        "SELECT value FROM ticks WHERE key=? AND ts_utc<=? ORDER BY ts_utc DESC LIMIT 1",
        (key, cutoff)
    ).fetchone()
    return None if r is None else float(r[0])

def peak_since(cn, key, since_epoch) -> Optional[float]:
    r = cn.execute("SELECT MAX(value) FROM ticks WHERE key=? AND ts_utc>=?", (key, since_epoch)).fetchone()
    return None if r is None or r[0] is None else float(r[0])

def peak_all_time(cn, key) -> Optional[float]:
    r = cn.execute("SELECT MAX(value) FROM ticks WHERE key=?", (key,)).fetchone()
    return None if r is None or r[0] is None else float(r[0])

def pct_change(curr, base) -> Optional[float]:
    if curr is None or base is None or base == 0: return None
    return (curr - base) / base * 100.0

def window_epochs(now=None) -> Dict[str, int]:
    if now is None:
        now = datetime.now(timezone.utc)
    return {
        "now": int(now.timestamp()),
        "h24": int((now - timedelta(hours=24)).timestamp()),
        "d7":  int((now - timedelta(days=7)).timestamp()),
        "d30": int((now - timedelta(days=30)).timestamp()),
    }

def set_kv(cn, k, v):
    cn.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))
    cn.commit()

def get_kv(cn, k) -> Optional[str]:
    r = cn.execute("SELECT v FROM kv WHERE k=?", (k,)).fetchone()
    return None if r is None else str(r[0])
