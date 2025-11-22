
# memento_field_expander.py
# Expands JSON fields from table.raw -> real columns.
# v2: supports both NAME-based and TYPE-based population.
# - Creates TEXT columns for each discovered field name.
# - Fills columns by exact field name.
# - For table 'umore', also ensures columns 'umore' and 'note' exist and
#   fills them by TYPE heuristic if name match is missing:
#     * umore := first field with type='rating' (or name='umore' if present)
#     * note  := first field with a text-like type
#
# Usage:
#   python memento_field_expander.py noutput.db umore
#   from memento_field_expander import expand_fields; expand_fields("noutput.db", "umore")
#
from __future__ import annotations

import sqlite3
import sys
from typing import List, Tuple, Dict

TEXTY_TYPES = ('text','textarea','note','long_text','multiline')

def _log(s: str) -> None:
    print(s, flush=True)

def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    ).fetchone()
    return bool(row)

def _has_raw_column(conn: sqlite3.Connection, table: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1].lower() == "raw" for r in rows)

def _list_current_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]

def _discover_field_names_and_types(conn: sqlite3.Connection, table: str) -> List[Tuple[str,str]]:
    sql = f"""
    SELECT DISTINCT
      COALESCE(json_extract(j.value,'$.name'), '') AS field_name,
      COALESCE(json_extract(j.value,'$.type'), '') AS field_type
    FROM {table} u,
         json_each(json_extract(u.raw,'$.fields')) AS j
    WHERE json_type(u.raw,'$.fields')='array'
    """
    out = []
    for name, ftype in conn.execute(sql):
        name = name or ''
        ftype = ftype or ''
        if name or ftype:
            out.append((name, ftype))
    return out

def _ensure_text_columns(conn: sqlite3.Connection, table: str, names: List[str]) -> List[str]:
    existing = set(_list_current_columns(conn, table))
    created = []
    for name in names:
        if name in existing or not name:
            continue
        col = '"' + name.replace('"', '""') + '"'  # quote
        conn.execute(f'ALTER TABLE {table} ADD COLUMN {col} TEXT')
        created.append(name)
    if created:
        conn.commit()
    return created

def _fill_column_by_name(conn: sqlite3.Connection, table: str, field: str) -> Tuple[int,int]:
    """Writes into column=field from the field with json name==field"""
    if not field:
        return (0,0)
    col = '"' + field.replace('"','""') + '"'
    sql_update = f"""
    UPDATE {table}
    SET {col} = (
      SELECT json_extract(j.value, '$.value')
      FROM json_each(json_extract({table}.raw,'$.fields')) AS j
      WHERE json_extract(j.value,'$.name') = ?
      LIMIT 1
    )
    WHERE ({col} IS NULL OR {col} = '')
      AND EXISTS (
        SELECT 1
        FROM json_each(json_extract({table}.raw,'$.fields')) AS j2
        WHERE json_extract(j2.value,'$.name') = ?
          AND json_extract(j2.value,'$.value') IS NOT NULL
          AND json_extract(j2.value,'$.value') <> ''
      );
    """
    cur = conn.cursor()
    cur.execute(sql_update, (field, field))
    updated = cur.rowcount if cur.rowcount is not None else 0

    sql_filled = f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL AND {col} <> ''"
    already = conn.execute(sql_filled).fetchone()[0]
    conn.commit()
    return updated, already

def _fill_column_by_type(conn: sqlite3.Connection, table: str, out_col: str, want_type: str, fallback_texty: bool=False) -> Tuple[int,int]:
    """
    Fill out_col from first field matching type=want_type.
    If fallback_texty=True and want_type not found, pick first text-like type.
    """
    col = '"' + out_col.replace('"','""') + '"'
    types_list = "', '".join(TEXTY_TYPES)
    # Prefer exact type first
    sql_update_type = f"""
    UPDATE {table}
    SET {col} = (
      SELECT json_extract(j.value, '$.value')
      FROM json_each(json_extract({table}.raw,'$.fields')) AS j
      WHERE json_extract(j.value,'$.type') = ?
      LIMIT 1
    )
    WHERE ({col} IS NULL OR {col} = '')
      AND EXISTS (
        SELECT 1
        FROM json_each(json_extract({table}.raw,'$.fields')) AS j2
        WHERE json_extract(j2.value,'$.type') = ?
          AND json_extract(j2.value,'$.value') IS NOT NULL
          AND json_extract(j2.value,'$.value') <> ''
      );
    """
    cur = conn.cursor()
    cur.execute(sql_update_type, (want_type, want_type))
    updated = cur.rowcount if cur.rowcount is not None else 0

    if fallback_texty and updated == 0:
        sql_update_texty = f"""
        UPDATE {table}
        SET {col} = (
          SELECT json_extract(j.value, '$.value')
          FROM json_each(json_extract({table}.raw,'$.fields')) AS j
          WHERE LOWER(COALESCE(json_extract(j.value,'$.type'),'')) IN ('{types_list}')
          LIMIT 1
        )
        WHERE ({col} IS NULL OR {col} = '')
          AND EXISTS (
            SELECT 1
            FROM json_each(json_extract({table}.raw,'$.fields')) AS j2
            WHERE LOWER(COALESCE(json_extract(j2.value,'$.type'),'')) IN ('{types_list}')
              AND json_extract(j2.value,'$.value') IS NOT NULL
              AND json_extract(j2.value,'$.value') <> ''
          );
        """
        cur.execute(sql_update_texty)
        updated = cur.rowcount if cur.rowcount is not None else 0

    sql_filled = f"SELECT COUNT(*) FROM {table} WHERE {col} IS NOT NULL AND {col} <> ''"
    already = conn.execute(sql_filled).fetchone()[0]
    conn.commit()
    return updated, already

def expand_fields(db_path: str, table: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        if not _table_exists(conn, table):
            _log(f"[expander] Tabella '{table}' inesistente, skip.")
            return
        if not _has_raw_column(conn, table):
            _log(f"[expander] Tabella '{table}' non ha colonna 'raw', skip.")
            return

        _log(f"[expander] Analisi tabella '{table}'...")
        nt = _discover_field_names_and_types(conn, table)
        names = [n for (n, t) in nt if n]
        type_summary: Dict[str,int] = {}
        for _, t in nt:
            t_low = (t or '').lower()
            if t_low:
                type_summary[t_low] = type_summary.get(t_low,0) + 1

        if not nt:
            _log(f"[expander] Nessun field trovato in {table}.")
            return

        _log(f"[expander] Campi rilevati: {', '.join(sorted(set(names)))}")
        if type_summary:
            _log(f"[expander] Tipi rilevati: " + ", ".join(f"{k}:{v}" for k,v in sorted(type_summary.items())))

        # Create columns for exact names
        created = _ensure_text_columns(conn, table, names)

        # Special-case: ensure 'umore' and 'note' columns in table 'umore'
        if table.lower() == 'umore':
            existing = set(_list_current_columns(conn, table))
            for extra in ('umore','note'):
                if extra not in existing:
                    conn.execute(f'ALTER TABLE {table} ADD COLUMN "{extra}" TEXT')
                    created.append(extra)
            conn.commit()

        if created:
            _log(f"[expander] Create colonne: {', '.join(created)}")
        else:
            _log(f"[expander] Nessuna nuova colonna: schema già allineato.")

        total_updated = 0

        # 1) Fill by exact name first
        for nm in names:
            updated, already = _fill_column_by_name(conn, table, nm)
            total_updated += updated
            if updated:
                _log(f"[expander] by-name '{nm}': aggiornati {updated}")

        # 2) For table 'umore': fill heuristics
        if table.lower() == 'umore':
            # umore: prefer name 'umore' (already done). If empty rows remain, fill first rating.
            updated_u, already_u = _fill_column_by_type(conn, table, 'umore', 'rating', fallback_texty=False)
            if updated_u:
                _log(f"[expander] by-type 'umore'←rating: aggiornati {updated_u}")
            # note: prefer name 'note' (already done). If empty rows remain, fill first text-like.
            updated_n, already_n = _fill_column_by_type(conn, table, 'note', 'text', fallback_texty=True)
            if updated_n:
                _log(f"[expander] by-type 'note'←text-like: aggiornati {updated_n}")

        _log(f"[expander] Completato: {table} → righe aggiornate totali {total_updated}.")
    finally:
        conn.close()

def main(argv: List[str]) -> int:
    if len(argv) < 2:
        print("Uso: python memento_field_expander.py <db_path> <table>", flush=True)
        return 2
    db_path = argv[0]
    table = argv[1]
    expand_fields(db_path, table)
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
