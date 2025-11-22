
# memento_import.py
# Importer with:
# - "enrich only NEW or MODIFIED rows" logic
# - proper incremental checkpoint (max remote timestamp)
# - SKIP NON-ACTIVE entries at import time (status != "active")
# - AUTO-RUN field expansion (memento_field_expander.expand_fields) after each section import
#
# Public entry:
#   memento_import_batch(db_path: str, batch_path: str) -> int
#
# INI options per section:
#   library_id      = <required>
#   table           = <defaults to section name>
#   tempo_col       = tempo (default)
#   sync            = incremental | full
#   limit           = 100
#   enrich_details  = 0 | auto | 1     (default: 1)
#   enrich_only_new = 1 | 0            (default: 1)  # kept for clarity; we also detect modified
#   enrich_workers  = 8
#   enrich_probe    = 20
#   enrich_max      = 0  (0 = no cap)
#
# Requires memento_sdk.py to expose:
#   fetch_all_entries_full(library_id, limit, progress=None)
#   fetch_incremental(library_id, modified_after_iso, limit, progress=None)
#   fetch_entry_detail(library_id, entry_id)
#
from __future__ import annotations

import json
import time
import sqlite3
import configparser
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- SDK imports (should be the patched one with include=fields) ---
from memento_sdk import (
    fetch_all_entries_full as _fetch_full,
    fetch_incremental as _fetch_inc,
    fetch_entry_detail as _fetch_detail,
)

# ------------------------------
# Logging
# ------------------------------

def _now_ts() -> str:
    return time.strftime("[%H:%M:%S]")

def _log(msg: str) -> None:
    print(f"{_now_ts()} {msg}", flush=True)

# ------------------------------
# DB helpers
# ------------------------------

def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS memento_sync (
        library_id TEXT PRIMARY KEY,
        last_modified_remote TEXT
    )""")
    conn.commit()

def _ensure_target_table(conn: sqlite3.Connection, table: str, tempo_col: str) -> None:
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {table} (
        ext_id TEXT PRIMARY KEY,
        {tempo_col} TEXT,
        raw     TEXT
    );""")
    conn.commit()

def _get_checkpoint(conn: sqlite3.Connection, library_id: str) -> Optional[str]:
    row = conn.execute(
        "SELECT last_modified_remote FROM memento_sync WHERE library_id = ?",
        (library_id,)
    ).fetchone()
    return row[0] if row and row[0] else None

def _set_checkpoint(conn: sqlite3.Connection, library_id: str, iso: Optional[str]) -> None:
    if iso:
        conn.execute("""
            INSERT INTO memento_sync (library_id, last_modified_remote)
            VALUES (?, ?)
            ON CONFLICT(library_id) DO UPDATE SET last_modified_remote=excluded.last_modified_remote
        """, (library_id, iso))
    else:
        conn.execute("DELETE FROM memento_sync WHERE library_id = ?", (library_id,))
    conn.commit()

# ------------------------------
# Row utilities
# ------------------------------

def _ext_id_for(e: Dict[str, Any]) -> str:
    # Your DB uses cloud id as ext_id; keep robust fallback
    return str(
        e.get("id")
        or e.get("entry_id")
        or e.get("uuid")
        or e.get("ext_id")
        or ""
    )

def _row_cloud_key(e: Dict[str, Any]) -> str:
    # Key used to compare remote rows with local rows
    return _ext_id_for(e)

def _tempo_from_row(e: Dict[str, Any], tempo_col: str) -> Optional[str]:
    if tempo_col in e:
        return str(e[tempo_col])
    for k in ("createdTime", "modifiedTime", "updatedTime", "time", "timestamp", "date"):
        v = e.get(k) or (e.get("raw") or {}).get(k)
        if v:
            return str(v)
    return None

def _status_of(e: Dict[str, Any]) -> Optional[str]:
    return (e.get("status") or (e.get("raw") or {}).get("status"))

def _is_active(e: Dict[str, Any]) -> bool:
    s = _status_of(e)
    return (s or "active").lower() == "active"

def _insert_rows(conn: sqlite3.Connection, table: str, tempo_col: str, rows: Sequence[Dict[str, Any]]) -> Tuple[int, int, int, int]:
    ins, missing, dup, skipped_non_active = 0, 0, 0, 0
    cur = conn.cursor()
    for e in rows:
        if not _is_active(e):
            skipped_non_active += 1
            continue
        ext = _ext_id_for(e)
        tempo = _tempo_from_row(e, tempo_col) or None
        if not tempo:
            missing += 1
            continue
        raw_json = json.dumps(e, ensure_ascii=False)
        try:
            cur.execute(
                f"INSERT OR IGNORE INTO {table} (ext_id, {tempo_col}, raw) VALUES (?,?,?)",
                (ext, tempo, raw_json)
            )
            if cur.rowcount == 1:
                ins += 1
            else:
                dup += 1
        except sqlite3.OperationalError as ex:
            if "no such table" in str(ex):
                _ensure_target_table(conn, table, tempo_col)
                cur.execute(
                    f"INSERT OR IGNORE INTO {table} (ext_id, {tempo_col}, raw) VALUES (?,?,?)",
                    (ext, tempo, raw_json)
                )
                if cur.rowcount == 1:
                    ins += 1
                else:
                    dup += 1
            else:
                raise
    conn.commit()
    return ins, missing, dup, skipped_non_active

# ------------------------------
# Fetch with logs
# ------------------------------

def _fetch_with_logs(kind: str, library_id: str, limit: int, last_iso: Optional[str]) -> List[Dict[str, Any]]:
    pages = 0
    total = 0

    def on_page(ev: Dict[str, Any]):
        nonlocal pages, total
        pages += 1
        total += int(ev.get("rows", 0))
        tag = "incremental" if kind == "incremental" else "full"
        extra = f" (updatedAfter={ev['updatedAfter']})" if "updatedAfter" in ev else ""
        _log(f"[{tag}] Pagina {pages} — {ev.get('rows', 0)} righe in {ev.get('sec','?')}s{extra}")

    if kind == "incremental":
        rows = _fetch_inc(library_id, modified_after_iso=last_iso, limit=limit, progress=on_page)
    else:
        rows = _fetch_full(library_id, limit=limit, progress=on_page)

    _log(f"[{kind}] Completato: {pages} pagine, {total} righe")
    return rows

# ------------------------------
# Enrichment helpers
# ------------------------------

def _truthy(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")

def _coerce_enrich_flag(v: Any):
    s = str(v or "").strip().lower()
    if s in ("0","false","no","off"):
        return False
    if s in ("auto",):
        return "auto"
    return True  # default: on

def _many_fields_empty(rows: Sequence[Dict[str, Any]]) -> bool:
    if not rows:
        return False
    empty = 0
    for e in rows:
        fields = isinstance(e, dict) and e.get("fields")
        if not fields:
            empty += 1
    return (empty / len(rows)) >= 0.8

def _remote_modified(e: Dict[str, Any]) -> Optional[str]:
    for k in ("modifiedTime", "updatedTime", "createdTime"):
        v = e.get(k) or (e.get("raw") or {}).get(k)
        if v:
            return str(v)
    return None

def _max_remote_iso(rows: Sequence[Dict[str, Any]]) -> Optional[str]:
    best = None
    for e in rows:
        z = _remote_modified(e)
        if z and (best is None or z > best):
            best = z
    return best

def _local_state_map(conn: sqlite3.Connection, table: str) -> Dict[str, Dict[str, Any]]:
    """
    Returns: { ext_id -> { 'modified': modifiedTime_locale (or None), 'has_fields': bool } }
    """
    out: Dict[str, Dict[str, Any]] = {}
    try:
        cur = conn.execute(f"""
            SELECT
              ext_id,
              json_extract(raw,'$.modifiedTime') AS modifiedTime,
              CASE
                WHEN json_type(raw,'$.fields')='array'
                 AND json_array_length(json_extract(raw,'$.fields'))>0
                THEN 1 ELSE 0
              END AS has_fields
            FROM {table}
        """)
        for ext_id, mod, hasf in cur.fetchall():
            if ext_id:
                out[str(ext_id)] = {"modified": (str(mod) if mod else None), "has_fields": bool(hasf)}
    except sqlite3.OperationalError:
        pass
    return out

def _enrich_rows_with_details_concurrent(
    library_id: str,
    rows: Sequence[Dict[str, Any]],
    workers: int = 8,
    probe: int = 20,
    max_details: int = 0,
) -> List[Dict[str, Any]]:
    """
    Fetch per-entry details concurrently.
    Returns the list of detailed JSONs for the given rows (order not guaranteed).
    """
    # Optional probe to check if details are actually richer
    sample = list(rows[:probe])
    richer = 0
    for e in sample:
        eid = _ext_id_for(e)
        if not eid:
            continue
        d = _fetch_detail(library_id, eid) or {}
        if d.get("fields"):
            richer += 1
    if probe > 0 and richer == 0:
        _log("Probe: nessun dettaglio più ricco → annullo arricchimento.")
        return []

    todo = list(rows)
    if max_details and max_details > 0:
        todo = todo[:max_details]

    out: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = {}
        for e in todo:
            eid = _ext_id_for(e)
            if not eid:
                continue
            futs[ex.submit(_fetch_detail, library_id, eid)] = eid
        done = 0
        for fut in as_completed(futs):
            try:
                d = fut.result() or {}
            except Exception:
                d = {}
            if d:
                out.append(d)
            done += 1
            if done % 50 == 0:
                _log(f"... dettagli fetch: {done}")
    return out

# ------------------------------
# Import section
# ------------------------------

def _import_section(
    conn: sqlite3.Connection,
    sect: str,
    cfg: Dict[str, str],
) -> Tuple[str, int]:
    """
    Imports one section from batch config.
    Returns (section_name, inserted_rows)
    """
    library_id = cfg["library_id"].strip()
    table      = cfg.get("table", sect).strip()
    tempo_col  = cfg.get("tempo_col", "tempo").strip()
    sync_mode  = (cfg.get("sync","incremental") or "incremental").strip().lower()
    limit      = int(cfg.get("limit","100").strip())

    # Enrichment flags
    enrich_details   = _coerce_enrich_flag(cfg.get("enrich_details", "1"))
    enrich_only_new  = _truthy(cfg.get("enrich_only_new", "1"))  # kept for config clarity
    enrich_workers   = int(cfg.get("enrich_workers","8").strip())
    enrich_probe     = int(cfg.get("enrich_probe","20").strip())
    enrich_max       = int(cfg.get("enrich_max","0").strip())

    _log(f"Sezione [{sect}] → tabella '{table}', libreria='{library_id}', sync='{sync_mode}', limit={limit}")
    _log(f"Config sezione [{sect}]: enrich_details={enrich_details}, enrich_only_new={enrich_only_new}, workers={enrich_workers}, probe={enrich_probe}, max={enrich_max}")

    _ensure_tables(conn)
    _ensure_target_table(conn, table, tempo_col)

    last_iso = None
    if sync_mode == "incremental":
        last_iso = _get_checkpoint(conn, library_id)
        _log(f"Checkpoint precedente (last_modified_remote): {last_iso or '— nessuno —'}")

    # Fetch
    kind = "incremental" if sync_mode == "incremental" else "full"
    rows = _fetch_with_logs(kind, library_id, limit, last_iso)

    # Compute new checkpoint from fetched rows (max remote modified/created)
    new_checkpoint = _max_remote_iso(rows)

    # Filter to ACTIVE only for downstream steps (skip 'trash' and others)
    total_before_filter = len(rows)
    rows = [e for e in rows if _is_active(e)]
    _log(f"[{sect}] Filtrati non-active: {total_before_filter - len(rows)} (mantengo solo 'active': {len(rows)})")

    # Decide enrichment
    if enrich_details is False:
        _log("Enrichment DISABILITATO da config → salto arricchimento.")
    else:
        # Skip enrichment entirely if the LIST already contains fields (fast path)
        needs_enrich = _many_fields_empty(rows)
        if enrich_details == "auto" and not needs_enrich:
            _log("Enrichment AUTO → non necessario (fields già presenti nella LIST).")
        else:
            # Build local state map ext_id -> {modified, has_fields}
            local_state = _local_state_map(conn, table)
            _log(f"[{sect}] Righe locali viste: {len(local_state)}")

            # Select candidates: ONLY new or modified (on ACTIVE rows only)
            candidates: List[Dict[str, Any]] = []
            skipped_unchanged = 0
            for e in rows:
                key = _row_cloud_key(e)
                if not key:
                    candidates.append(e)
                    continue
                st = local_state.get(key)
                if st is None:
                    # NEW
                    candidates.append(e)
                    continue
                # Present locally: enrich only if remote is newer
                r_mod = _remote_modified(e)
                l_mod = st["modified"]
                if r_mod and (l_mod is None or r_mod > l_mod):
                    candidates.append(e)  # UPDATED
                else:
                    skipped_unchanged += 1

            _log(f"[{sect}] Candidati per arricchimento: {len(candidates)} (saltate per identicità: {skipped_unchanged})")

            if candidates:
                enriched = _enrich_rows_with_details_concurrent(
                    library_id, candidates,
                    workers=enrich_workers, probe=enrich_probe, max_details=enrich_max
                )
                # merge per ext_id
                by_id = { _row_cloud_key(e): e for e in enriched }
                rows = [ by_id.get(_row_cloud_key(r), r) for r in rows ]
            else:
                _log("Nessun candidato da arricchire (tutti nuovi già completi o invariati).")

    # Insert (defensive skip non-active also inside)
    _log("Costruzione payload SQLite…")
    ins, miss, dup, skipped_non_active = _insert_rows(conn, table, tempo_col, rows)
    _log(f"Inserite {ins}/{len(rows)} (saltate senza tempo: {miss}, ignorate come duplicati: {dup}, skippate non-active: {skipped_non_active})")

    # Update checkpoint (incremental)
    if sync_mode == "incremental":
        if new_checkpoint:
            _set_checkpoint(conn, library_id, new_checkpoint)
            _log(f"Aggiorno checkpoint a: {new_checkpoint}")
        else:
            _log("Nessun timestamp remoto rilevato: checkpoint invariato.")

    # ------------------------------
    # AUTO-RUN FIELD EXPANDER
    # ------------------------------
    try:
        db_file = conn.execute("PRAGMA database_list").fetchall()[0][2]
    except Exception:
        db_file = None
    if db_file:
        try:
            from memento_field_expander import expand_fields
            _log(f"[{sect}] Avvio expander campi → tabella '{table}'")
            expand_fields(db_file, table)
        except Exception as ex:
            _log(f"[{sect}] Expander non riuscito: {ex}")
    else:
        _log(f"[{sect}] Impossibile determinare il percorso DB per l'expander.")

    _log(f"Fine sezione [{sect}] → tentate: {len(rows)}, inserite: {ins}.")
    return sect, ins

# ------------------------------
# Batch reader and entry point
# ------------------------------

def _read_batch(batch_path: str) -> Dict[str, Dict[str,str]]:
    """
    Reads an INI or YAML file and returns a dict: {section_name: {key: value}}.
    """
    p = Path(batch_path)
    if not p.exists():
        raise FileNotFoundError(f"Batch file not found: {batch_path}")

    if p.suffix.lower() in (".ini", ".cfg"):
        cp = configparser.ConfigParser()
        cp.read(batch_path, encoding="utf-8")
        return { sect: {k:v for k,v in cp.items(sect)} for sect in cp.sections() }

    if p.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml  # type: ignore
            with open(batch_path, "r", encoding="utf-8") as fh:
                y = yaml.safe_load(fh) or {}
            out: Dict[str, Dict[str,str]] = {}
            for sect, vals in (y.items() if isinstance(y, dict) else []):
                if isinstance(vals, dict):
                    out[sect] = {str(k): str(v) for k,v in vals.items()}
            return out
        except Exception as ex:
            raise RuntimeError(f"Errore lettura YAML: {ex}")

    # Fallback: treat as INI
    cp = configparser.ConfigParser()
    cp.read(batch_path, encoding="utf-8")
    return { sect: {k:v for k,v in cp.items(sect)} for sect in cp.sections() }

def _import_from_ini(conn: sqlite3.Connection, sections: Dict[str, Dict[str,str]]) -> int:
    total_inserted = 0
    for sect, cfg in sections.items():
        _, n = _import_section(conn, sect, cfg)
        total_inserted += n
    return total_inserted

def memento_import_batch(db_path: str, batch_path: str) -> int:
    """
    Main entry point used by your menu.
    Returns total rows inserted across all sections.
    """
    db_path = (db_path or "noutput.db").strip()
    batch_path = (batch_path or "memento_import.ini").strip()
    _log(f"== Import batch avviato == DB: {db_path}  Batch: {batch_path}")
    sections = _read_batch(batch_path)
    if not sections:
        _log("Nessuna sezione trovata nel batch.")
        return 0
    conn = sqlite3.connect(db_path)
    try:
        total = _import_from_ini(conn, sections)
        _log("== Import batch terminato ==")
        return total
    finally:
        conn.close()
