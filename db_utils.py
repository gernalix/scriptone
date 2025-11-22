import sqlite3
from typing import Set

def quote_ident(name: str) -> str:
    if name is None:
        raise ValueError("Identifier cannot be None")
    return '"' + str(name).replace('"', '""') + '"'

def table_columns(conn: sqlite3.Connection, table: str) -> Set[str]:
    cur = conn.execute(f"PRAGMA table_info({quote_ident(table)})")
    return {row[1] for row in cur.fetchall()}

def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def ensure_ext_id(conn: sqlite3.Connection, table: str) -> str:
    cols = table_columns(conn, table)
    if 'ext_id' not in cols:
        conn.execute(f"ALTER TABLE {quote_ident(table)} ADD COLUMN ext_id TEXT")
        try:
            idx = f"{table}_ext_id_unique"
            conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {quote_ident(idx)} ON {quote_ident(table)} (ext_id)")
        except sqlite3.OperationalError:
            pass
        conn.commit()
    return 'ext_id'

def ensure_pragmas(conn: sqlite3.Connection):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-20000")
    conn.commit()

def ensure_indexes(conn: sqlite3.Connection, table: str, tempo_col: str):
    try:
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_{tempo_col} ON {quote_ident(table)} ({quote_ident(tempo_col)})")
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS {quote_ident(table + '_ext_id_unique')} ON {quote_ident(table)} (ext_id)")
    except sqlite3.OperationalError:
        pass
    conn.commit()

def ensure_sync_table(conn: sqlite3.Connection):
    conn.execute(
"""
CREATE TABLE IF NOT EXISTS memento_sync (
  library_id TEXT PRIMARY KEY,
  last_modified_remote TEXT,
  last_run_utc TEXT
);
"""
)
    conn.commit()
