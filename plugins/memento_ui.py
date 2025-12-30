# v8
# -*- coding: utf-8 -*-
# v8 - Robust CSV type inference (m/d/yy), safer boolean detection, larger samples

import os
import csv
import re
import hashlib
import sqlite3
from urllib.parse import unquote, quote
from datetime import datetime, date

from datasette import hookimpl
from datasette.utils.asgi import Response


BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CSV_DIR = os.path.join(BASE_DIR, "memento_csvs")
DB_PATH = None  # resolved at runtime from the default Datasette database

# ---- overrides: columns that must NOT be treated as booleans ------------------

_NON_BOOL_CACHE = None

def _load_non_boolean_overrides() -> dict:
    """Parse BASE_DIR/not_booleans.txt.

    Format (one per line):
        table_name: col1, col2, col3

    Returns:
        {table_name_lower: {colname_lower, ...}, ...}
    """
    global _NON_BOOL_CACHE
    if _NON_BOOL_CACHE is not None:
        return _NON_BOOL_CACHE

    path = os.path.join(BASE_DIR, "not_booleans.txt")
    overrides: dict = {}
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if ":" not in line:
                    continue
                t, cols = line.split(":", 1)
                t = t.strip().lower()
                if not t:
                    continue
                colset = overrides.setdefault(t, set())
                for c in cols.split(","):
                    cc = c.strip()
                    if cc:
                        colset.add(cc.lower())

    _NON_BOOL_CACHE = overrides
    return overrides

def _is_forced_non_boolean(table_name: str, colname: str) -> bool:
    if not table_name or not colname:
        return False
    ov = _load_non_boolean_overrides()
    tn = table_name.strip().lower()
    cn = colname.strip().lower()
    return (tn in ov) and (cn in ov[tn])

# ---- util: quoting identifiers ------------------------------------------------

def _q(ident: str) -> str:
    # Quote SQLite identifier with double quotes
    return '"' + ident.replace('"', '""') + '"'

# ---- type inference -----------------------------------------------------------

_BOOL_TRUE = {"1", "true", "t", "yes", "y", "on"}
_BOOL_FALSE = {"0", "false", "f", "no", "n", "off"}

_DURATION_RE = re.compile(r"^\s*(\d{1,3})\s*:\s*(\d{2})\s*$")

_DATE_FORMATS = (
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%Y/%m/%d",
    "%m/%d/%y",
    "%m/%d/%Y",
    "%m-%d-%y",
    "%m-%d-%Y",
)
_DATETIME_FORMATS = (
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%m/%d/%y %H:%M",
    "%m/%d/%Y %H:%M",
    "%m/%d/%y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%m-%d-%y %H:%M",
    "%m-%d-%Y %H:%M",
    "%m-%d-%y %H:%M:%S",
    "%m-%d-%Y %H:%M:%S",
)
def _try_parse_bool(s: str):
    x = s.strip().lower()
    if x in _BOOL_TRUE:
        return 1
    if x in _BOOL_FALSE:
        return 0
    return None

def _try_parse_int(s: str):
    x = s.strip()
    if x == "":
        return None
    try:
        if re.match(r"^[+-]?\d+$", x):
            return int(x)
    except Exception:
        pass
    return None

def _try_parse_float(s: str):
    x = s.strip()
    if x == "":
        return None
    try:
        # accept comma decimals too
        x2 = x.replace(",", ".")
        return float(x2)
    except Exception:
        return None

def _try_parse_date(s: str):
    x = s.strip()
    if x == "":
        return None
    # ISO date first
    try:
        # fromisoformat accepts YYYY-MM-DD
        d = date.fromisoformat(x)
        return d
    except Exception:
        pass
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(x, fmt).date()
        except Exception:
            continue
    return None

def _try_parse_datetime(s: str):
    x = s.strip()
    if x == "":
        return None
    # ISO first (supports "YYYY-MM-DDTHH:MM:SS(.fff)" and "YYYY-MM-DD HH:MM:SS")
    try:
        # normalize 'Z' -> +00:00 (keep as aware, but we'll store ISO)
        if x.endswith("Z"):
            x2 = x[:-1] + "+00:00"
            return datetime.fromisoformat(x2)
        return datetime.fromisoformat(x)
    except Exception:
        pass
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(x, fmt)
        except Exception:
            continue
    return None

def _try_parse_duration_hhmm(s: str):
    m = _DURATION_RE.match(s or "")
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    if 0 <= mm <= 59:
        return hh, mm
    return None


def _guess_widget_from_name(colname: str):
    n = (colname or "").strip().lower()
    if not n:
        return None
    # datetime-ish
    datetime_tokens = ["datetime", "timestamp", "quando", "ora", "orario", "time", "created", "updated", "modified", "at"]
    date_tokens = ["date", "data", "giorno", "day"]
    # prioritize datetime tokens
    if any(tok in n for tok in datetime_tokens):
        return ("datetime", "datetime")
    if any(tok in n for tok in date_tokens):
        return ("date", "date")
    return None

def infer_schema_from_csv(csv_path: str, table_name: str = None, sample_rows: int = 5000):
    """
    Returns:
        columns: list of dict: {name, sql_type, widget, subtype}
    """
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        # init stats
        stats = {c: {
            "nonempty": 0,
            "bool_ok": 0,
            "int_ok": 0,
            "float_ok": 0,
            "date_ok": 0,
            "datetime_ok": 0,
            "duration_ok": 0,
        } for c in fieldnames}

        rows_seen = 0
        for row in reader:
            rows_seen += 1
            for c in fieldnames:
                v = row.get(c, "")
                if v is None:
                    v = ""
                s = str(v)
                if s.strip() == "":
                    continue
                st = stats[c]
                st["nonempty"] += 1

                if _try_parse_bool(s) is not None:
                    st["bool_ok"] += 1
                if _try_parse_int(s) is not None:
                    st["int_ok"] += 1
                if _try_parse_float(s) is not None:
                    st["float_ok"] += 1
                if _try_parse_date(s) is not None:
                    st["date_ok"] += 1
                if _try_parse_datetime(s) is not None:
                    st["datetime_ok"] += 1
                if _try_parse_duration_hhmm(s) is not None:
                    st["duration_ok"] += 1
            if rows_seen >= sample_rows:
                break

    columns = []
    for c in fieldnames:
        st = stats[c]
        n = st["nonempty"]
        # default
        sql_type = "TEXT"
        widget = "text"
        subtype = None

        def ratio(k):
            return (st[k] / n) if n else 0.0

        # Decide based on strong ratios
        # If column is explicitly marked as non-boolean, never pick the checkbox widget.
        if n and stats[c]['nonempty'] >= 10 and ratio("bool_ok") >= 0.95 and not _is_forced_non_boolean(table_name or "", c):
            sql_type = "INTEGER"
            widget = "checkbox"
            subtype = "boolean"
        elif n and ratio("int_ok") >= 0.95:
            sql_type = "INTEGER"
            widget = "number"
            subtype = "integer"
        elif n and ratio("float_ok") >= 0.95:
            sql_type = "REAL"
            widget = "number"
            subtype = "float"
        elif n and ratio("datetime_ok") >= 0.80:
            sql_type = "TEXT"
            widget = "datetime"
            subtype = "datetime"
        elif n and ratio("date_ok") >= 0.80:
            sql_type = "TEXT"
            widget = "date"
            subtype = "date"
        elif n and ratio("duration_ok") >= 0.80:
            sql_type = "TEXT"
            widget = "duration_hhmm"
            subtype = "duration_hhmm"
        else:
            sql_type = "TEXT"
            widget = "text"
            subtype = "text"
        # Name-based fallback for date/datetime when values are empty or inconclusive
        if widget == "text":
            g = _guess_widget_from_name(c)
            if g:
                widget, subtype = g
                sql_type = "TEXT"

        columns.append({"name": c, "sql_type": sql_type, "widget": widget, "subtype": subtype})

    return columns

# ---- import -------------------------------------------------------------------

def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def ensure_memento_meta_tables(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "__memento_import_state" (
            csv_name TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            sha256 TEXT NOT NULL,
            imported_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "__memento_column_meta" (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            widget TEXT NOT NULL,
            subtype TEXT,
            PRIMARY KEY (table_name, column_name)
        )
    """)

def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table_name,))
    return cur.fetchone() is not None

def import_csv_into_sqlite(conn: sqlite3.Connection, csv_path: str, table_name: str):
    columns = infer_schema_from_csv(csv_path, table_name=table_name)

    # create table
    cols_sql = ", ".join([f"{_q(c['name'])} {c['sql_type']}" for c in columns])
    conn.execute(f'CREATE TABLE IF NOT EXISTS {_q(table_name)} ({cols_sql})')

    # store widget metadata
    for c in columns:
        conn.execute(
            'INSERT OR REPLACE INTO "__memento_column_meta"(table_name, column_name, widget, subtype) VALUES (?, ?, ?, ?)',
            (table_name, c["name"], c["widget"], c["subtype"])
        )

    # import rows
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        placeholders = ", ".join(["?"] * len(fieldnames))
        insert_sql = f'INSERT INTO {_q(table_name)} ({", ".join(_q(c) for c in fieldnames)}) VALUES ({placeholders})'

        for row in reader:
            vals = []
            for c in fieldnames:
                v = row.get(c, "")
                if v is None:
                    v = ""
                s = str(v).strip()
                if s == "":
                    vals.append(None)
                else:
                    # Normalize some types
                    meta = next((x for x in columns if x["name"] == c), None)
                    if meta and meta["subtype"] == "boolean":
                        bv = _try_parse_bool(s)
                        vals.append(bv if bv is not None else None)
                    elif meta and meta["subtype"] == "integer":
                        iv = _try_parse_int(s)
                        vals.append(iv if iv is not None else s)
                    elif meta and meta["subtype"] == "float":
                        fv = _try_parse_float(s)
                        vals.append(fv if fv is not None else s)
                    elif meta and meta["subtype"] == "date":
                        dv = _try_parse_date(s)
                        vals.append(dv.isoformat() if dv else s)
                    elif meta and meta["subtype"] == "datetime":
                        dt = _try_parse_datetime(s)
                        vals.append(dt.isoformat() if dt else s)
                    elif meta and meta["subtype"] == "duration_hhmm":
                        # keep canonical HH:MM
                        d = _try_parse_duration_hhmm(s)
                        if d:
                            vals.append(f"{d[0]}:{d[1]:02d}")
                        else:
                            vals.append(s)
                    else:
                        vals.append(s)
            conn.execute(insert_sql, vals)

def ensure_imported_from_csvs(db_path: str):
    if not os.path.isdir(CSV_DIR) or not os.path.isfile(db_path):
        return

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        ensure_memento_meta_tables(conn)

        for name in sorted(os.listdir(CSV_DIR)):
            if not name.lower().endswith(".csv"):
                continue
            csv_path = os.path.join(CSV_DIR, name)
            table_name = os.path.splitext(name)[0]

            sha = _sha256_file(csv_path)
            cur = conn.execute('SELECT sha256 FROM "__memento_import_state" WHERE csv_name = ?', (name,))
            row = cur.fetchone()
            if row:
                # already imported that exact file
                if row[0] == sha:
                    continue
                # if hash differs, do not auto-reimport to avoid overwriting user data
                continue

            if not table_exists(conn, table_name):
                import_csv_into_sqlite(conn, csv_path, table_name)
            else:
                # table exists - do not touch (user may have already populated it)
                pass

            conn.execute(
                'INSERT OR REPLACE INTO "__memento_import_state"(csv_name, table_name, sha256, imported_at) VALUES (?, ?, ?, ?)',
                (name, table_name, sha, datetime.utcnow().isoformat() + "Z")
            )

        conn.commit()
    finally:
        conn.close()

# ---- Datasette UI -------------------------------------------------------------

async def _get_memento_tables(datasette):
    db = datasette.get_database()
    # Prefer tables tracked in import state; fallback to any non-internal tables that have csv counterpart
    try:
        rows = await db.execute('SELECT table_name, csv_name, imported_at FROM "__memento_import_state" ORDER BY table_name')
        if rows.rows:
            return [{"table_name": r[0], "csv_name": r[1], "imported_at": r[2]} for r in rows.rows]
    except Exception:
        pass

    tables = []
    if os.path.isdir(CSV_DIR):
        for name in sorted(os.listdir(CSV_DIR)):
            if name.lower().endswith(".csv"):
                tables.append({"table_name": os.path.splitext(name)[0], "csv_name": name, "imported_at": None})
    return tables

async def _get_column_meta(datasette, table_name: str):
    db = datasette.get_database()
    cols = []
    # PRAGMA table_info is easiest via execute
    info = await db.execute(f'PRAGMA table_info({_q(table_name)})')
    # meta
    meta_map = {}
    try:
        meta = await db.execute('SELECT column_name, widget, subtype FROM "__memento_column_meta" WHERE table_name = ?', (table_name,))
        for r in meta.rows:
            meta_map[r[0]] = {"widget": r[1], "subtype": r[2]}
    except Exception:
        pass

    for r in info.rows:
        # r: cid, name, type, notnull, dflt_value, pk
        name = r[1]
        col_type = (r[2] or "").upper()
        w = meta_map.get(name, {}).get("widget") or "text"
        st = meta_map.get(name, {}).get("subtype")

        # Hard override: some columns are known to be NOT boolean, even if
        # values look like 0/1. In that case, never render as checkbox.
        if _is_forced_non_boolean(table_name, name) and w == "checkbox":
            if "INT" in col_type or col_type == "INTEGER":
                w, st = "number", "integer"
            elif "REAL" in col_type or "FLOA" in col_type or "DOUB" in col_type:
                w, st = "number", "float"
            else:
                # still allow name-based date/datetime inference
                g = _guess_widget_from_name(name)
                if g:
                    w, st = g
                else:
                    w, st = "text", "text"

        # If widget unknown/plain text, try to autodetect from existing data (and column name)
        if w in (None, "", "text"):
            # 1) name heuristic
            g = _guess_widget_from_name(name)
            if g:
                w, st = g
            else:
                # 2) sample values
                try:
                    sample = await db.execute(
                        f'SELECT {_q(name)} FROM {_q(table_name)} '
                        f'WHERE {_q(name)} IS NOT NULL AND TRIM(CAST({_q(name)} AS TEXT)) != "" '
                        f'LIMIT 20'
                    )
                    vals = [r[0] for r in sample.rows]
                    # try datetime/date/duration/bool in that order
                    if any(_try_parse_datetime(v) is not None for v in vals):
                        w, st = "datetime", "datetime"
                    elif any(_try_parse_date(v) is not None for v in vals):
                        w, st = "date", "date"
                    elif any(_try_parse_duration_hhmm(v) is not None for v in vals):
                        w, st = "duration_hhmm", "duration_hhmm"
                    elif (not _is_forced_non_boolean(table_name, name)) and any(_try_parse_bool(v) is not None for v in vals):
                        w, st = "checkbox", "boolean"
                except Exception:
                    pass
            # persist autodetected widget for next time
            try:
                await db.execute_write(
                    'INSERT OR REPLACE INTO "__memento_column_meta"(table_name, column_name, widget, subtype) VALUES (?, ?, ?, ?)',
                    (table_name, name, w or "text", st),
                )
            except Exception:
                pass
        cols.append({
            "name": name,
            "type": col_type,
            "widget": w,
            "subtype": st,
            "notnull": bool(r[3]),
            "pk": bool(r[5]),
        })
    return cols

def _normalize_duration_inputs(form, colname):
    h = form.get(f"{colname}__hh")
    m = form.get(f"{colname}__mm")
    if h is None and m is None:
        return None
    try:
        hh = int(h or 0)
        mm = int(m or 0)
        if mm < 0 or mm > 59:
            return None
        return f"{hh}:{mm:02d}"
    except Exception:
        return None

@hookimpl
def startup(datasette):
    # Ensure tables exist before serving
    try:
        db_path = datasette.get_database().path
    except Exception:
        db_path = os.path.join(BASE_DIR, "output.db")
    ensure_imported_from_csvs(db_path)

@hookimpl
def register_routes():
    return [
        (r"^/memento$", memento_home),
        (r"^/memento/(?P<table>.+)/insert$", memento_insert),
    ]

async def memento_home(request, datasette):
    tables = await _get_memento_tables(datasette)
    db = datasette.get_database()
    ctx = {
        "tables": [
            {
                **t,
                "view_url": datasette.urls.table(db.name, t["table_name"]),
                "insert_url": datasette.urls.path("/memento/" + quote(t["table_name"], safe="") + "/insert"),
            }
            for t in tables
        ],
        "db_name": db.name,
    }
    html = await datasette.render_template("memento_home.html", ctx, request=request)
    return Response.html(html)

async def memento_insert(request, datasette):
    db = datasette.get_database()
    table_raw = (getattr(request, 'url_vars', None) or request.scope.get('url_vars', {})).get('table')
    table = unquote(table_raw) if table_raw else None
    if not table:
        return Response.text('Missing table', status=400)
    message = None

    # POST -> insert
    if request.method == "POST":
        form = await request.post_vars()
        cols = await _get_column_meta(datasette, table)

        names = []
        values = []
        for c in cols:
            colname = c["name"]
            if c["pk"]:
                # do not force user to set PK
                continue

            if c["widget"] == "duration_hhmm":
                v = _normalize_duration_inputs(form, colname)
            elif c["widget"] == "checkbox":
                v = 1 if form.get(colname) in ("on", "1", "true", "True") else 0
            else:
                v = form.get(colname)

            if v is None or str(v).strip() == "":
                values.append(None)
            else:
                s = str(v).strip()
                # Normalize datetime-local -> ISO
                if c["widget"] == "datetime":
                    try:
                        # HTML datetime-local gives 'YYYY-MM-DDTHH:MM'
                        dt = datetime.fromisoformat(s)
                        s = dt.isoformat()
                    except Exception:
                        pass
                elif c["widget"] == "date":
                    try:
                        d = date.fromisoformat(s)
                        s = d.isoformat()
                    except Exception:
                        pass
                values.append(s)
            names.append(colname)

        if names:
            sql = f'INSERT INTO {_q(table)} ({", ".join(_q(n) for n in names)}) VALUES ({", ".join(["?"]*len(names))})'
            await db.execute_write(sql, values, block=True)
            try:
                datasette.add_message(request, f"Record inserito in '{table}'.")
            except Exception:
                pass
            return Response.redirect(datasette.urls.table(db.name, table))
        else:
            message = "Nessun dato da inserire."

    # GET -> show form
    cols = await _get_column_meta(datasette, table)
    ctx = {
        "table": table,
        "columns": cols,
        "message": message,
        "table_url": datasette.urls.table(db.name, table),
    }
    html = await datasette.render_template("memento_insert.html", ctx, request=request)
    return Response.html(html)

@hookimpl
def menu_links(datasette, actor, request):
    return [
        {"href": datasette.urls.path("/memento"), "label": "Memento"},
    ]