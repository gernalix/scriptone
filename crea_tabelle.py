# -*- coding: utf-8 -*-
# crea_tabelle.py — versione completa con menu Modifica, sweep tmp, rebuild safe e diagnostica

import sys
import sqlite3
from pathlib import Path
import re
from typing import List, Tuple, Optional, Dict
from util import resolve_here
from memento_import import memento_import_batch


# Percorsi
HERE = Path(__file__).resolve().parent
DB_PATH = HERE / "noutput.db"
TRIGGERS_PATH = HERE / "triggers.sql"
CONFIG_PATH = HERE / "crea_tabelle.config.json"

# Switch: se True, crea una NUOVA connessione per OGNI azione nel sottomenu "Modifica tabella".
# Se False, mantiene una singola connessione per tutta la durata del sottomenu.
REOPEN_PER_ACTION = False

# --------------------------- Utilità console ---------------------------

def pause(msg: str = "\nPremi Invio per uscire..."):
    try:
        input(msg)
    except EOFError:
        pass

def print_header(title: str):
    line = "=" * 64
    print(f"\n{line}\n{title}\n{line}")

def ask(prompt: str) -> str:
    try:
        return input(prompt)
    except EOFError:
        return ""

def mode_label() -> str:
    return "PER-AZIONE (riuso=NO)" if REOPEN_PER_ACTION else "RIUSO (riuso=SÌ)"

# --------------------------- Config persistence ---------------------------
import json

def load_config() -> dict:
    cfg = {}
    try:
        if CONFIG_PATH.exists():
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception:
        cfg = {}
    return cfg

def save_config(cfg: dict) -> None:
    try:
        CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[WARN] Impossibile salvare {CONFIG_PATH.name}: {e}")

def _init_mode_from_config():
    global REOPEN_PER_ACTION
    cfg = load_config()
    if isinstance(cfg.get("reopen_per_action"), bool):
        REOPEN_PER_ACTION = cfg["reopen_per_action"]

# --------------------------- DB helpers ---------------------------

def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA recursive_triggers = ON;")
    return conn

def sweep_stale_tmp_tables():
    """Rimuove eventuali tabelle temporanee __tmp__rebuild residue da tentativi falliti."""
    try:
        with connect_db() as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%__tmp__rebuild'"
            ).fetchall()
            for r in rows:
                tname = r["name"]
                try:
                    conn.execute(f'DROP TABLE IF EXISTS "{tname}"')
                    print(f"[CLEANUP] Rimossa tabella temporanea residua: {tname}")
                except Exception as e:
                    print(f"[WARN] Impossibile rimuovere tmp residua {tname}: {e}")
    except Exception:
        pass

def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (table,)).fetchone()
    return row is not None

def list_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
    ).fetchall()
    return [r["name"] for r in rows]

def get_columns_info(conn: sqlite3.Connection, table: str) -> List[sqlite3.Row]:
    return conn.execute(f"PRAGMA table_info('{table}')").fetchall()

def get_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [r["name"] for r in get_columns_info(conn, table)]

def get_fks(conn: sqlite3.Connection, table: str) -> List[sqlite3.Row]:
    return conn.execute(f"PRAGMA foreign_key_list('{table}')").fetchall()

def get_index_sqls(conn: sqlite3.Connection, table: str) -> List[str]:
    idx_rows = conn.execute(f"PRAGMA index_list('{table}')").fetchall()
    sqls: List[str] = []
    for r in idx_rows:
        name = r["name"]
        src = conn.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name=?", (name,)).fetchone()
        if src and src["sql"]:
            sqls.append(src["sql"])
    return sqls

def get_index_create_statements(conn: sqlite3.Connection, table: str) -> List[str]:
    """Compat: restituisce le CREATE INDEX per la tabella.
    Alias di get_index_sqls per il resto del codice."""
    return get_index_sqls(conn, table)


def drop_trigger_if_exists(conn: sqlite3.Connection, name: str):
    row = conn.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=?;", (name,)).fetchone()
    if row:
        conn.execute(f'DROP TRIGGER "{name}"')

# --------------------------- Diagnostica errori ---------------------------

def _fk_violations_report(conn: sqlite3.Connection, table: str, limit: int = 10) -> str:
    try:
        rows = conn.execute(f"PRAGMA foreign_key_check('{table}')").fetchall()
    except Exception:
        rows = []
    if not rows:
        return "Nessuna violazione FK rilevata da PRAGMA foreign_key_check."
    out = [f"Violazioni FK su '{table}' (max {limit}):"]
    for i, r in enumerate(rows[:limit], 1):
        try:
            t = r[0]; rowid = r[1]; parent = r[2]
        except Exception:
            t = getattr(r, 'table', '?'); rowid = getattr(r, 'rowid', '?'); parent = getattr(r, 'parent', '?')
        out.append(f"  {i}) tabella={t} rowid={rowid} parent={parent}")
        try:
            rec = conn.execute(f'SELECT * FROM "{t}" WHERE rowid=?', (rowid,)).fetchone()
            if rec is not None:
                cols = rec.keys()
                data = ', '.join([f'{c}={rec[c]!r}' for c in cols])
                out.append(f"     → riga: {data}")
        except Exception:
            pass
    more = len(rows) - min(len(rows), limit)
    if more > 0:
        out.append(f"  ... e altre {more} violazioni")
    return "\n".join(out)

def _unique_violations_report(conn: sqlite3.Connection, table: str, cols: list, sample: int = 5) -> str:
    out = [f"Possibili duplicati che impediscono UNIQUE su {cols}:"]
    for c in cols:
        try:
            q = f'SELECT "{c}", COUNT(*) as cnt FROM "{table}" GROUP BY "{c}" HAVING cnt>1 ORDER BY cnt DESC LIMIT {sample}'
            dups = conn.execute(q).fetchall()
            if dups:
                for val, cnt in dups:
                    out.append(f"  col={c} val={val!r} cnt={cnt}")
            else:
                out.append(f"  col={c} → nessun duplicato evidente (forse NULL multipli, consentiti)")
        except Exception as e:
            out.append(f"  col={c} → errore nel conteggio: {e}")
    return "\n".join(out)

# --------------------------- Trigger handling ---------------------------

def alias_column(col: str) -> str:
    return col

def build_json_pairs(cols: List[str], prefix: str) -> str:
    pairs = []
    for c in cols:
        pairs.append(f"'{alias_column(c)}'")
        pairs.append(f'{prefix}."{c}"')
    return ", ".join(pairs)

def read_triggers_template() -> str:
    if not TRIGGERS_PATH.exists():
        raise FileNotFoundError(f"File triggers.sql non trovato: {TRIGGERS_PATH}")
    return TRIGGERS_PATH.read_text(encoding="utf-8")

def render_triggers_sql(template: str, table: str, cols: List[str], logseq_col: str = "logseq") -> str:
    json_new_pairs = build_json_pairs(cols, "NEW")
    json_old_pairs = build_json_pairs(cols, "OLD")

    ctx = {
        "table": table,
        "trigger_name_insert": f'audit__{table}__insert',
        "trigger_name_update": f'audit__{table}__update',
        "trigger_name_delete": f'audit__{table}__delete',
        "trigger_name_logseq": f'set_logseq_random__{table}',  # compat, verrà rimosso
        "json_new_pairs": json_new_pairs,
        "json_old_pairs": json_old_pairs,
        "logseq_col": logseq_col,
    }

    sql = template.format(**ctx)

    # Rimuovi eventuale blocco/trigger set_logseq_random
    sql = re.sub(r"-- === SET LOGSEQ RANDOM ================================.*?(?=(-- ===|$))", "", sql, flags=re.DOTALL)
    trig_name = ctx["trigger_name_logseq"]
    sql = re.sub(rf"CREATE\s+TRIGGER\s+{re.escape(trig_name)}.*?END;", "", sql, flags=re.DOTALL | re.IGNORECASE)

    return sql.strip()

def apply_triggers_to_table(conn: sqlite3.Connection, table: str):
    cols = get_columns(conn, table)
    template = read_triggers_template()
    sql = render_triggers_sql(template, table, cols, logseq_col="logseq")

    trig_names = [
        f'audit__{table}__insert',
        f'audit__{table}__update',
        f'audit__{table}__delete',
        f'set_logseq_random__{table}'
    ]
    with conn:
        for tn in trig_names:
            drop_trigger_if_exists(conn, tn)
        conn.executescript(sql)
    print(f"[OK] Trigger applicati per tabella '{table}'.")

# --------------------------- Parsing colonne ---------------------------

TYPE_MAP = {"i": "INTEGER", "r": "REAL", "t": "TEXT"}

def parse_columns_spec(spec: str) -> List[Tuple[str, str, bool]]:
    out: List[Tuple[str, str, bool]] = []
    if not spec.strip():
        return out
    for p in [p.strip() for p in spec.split(",")]:
        if not p:
            continue
        m = re.match(r'^([A-Za-z_][A-Za-z0-9_]*)\s+([irtIRT])([zZ])?$', p)
        if not m:
            raise ValueError(f"Formato colonna non valido: '{p}'. Usa es: 'nome t' oppure 'esempio iz'")
        name = m.group(1)
        tcode = m.group(2).lower()
        has_z = bool(m.group(3))
        sql_type = TYPE_MAP[tcode]
        default_zero = has_z and (sql_type in ("INTEGER", "REAL"))
        out.append((name, sql_type, default_zero))
    return out

# --------------------------- Creazione tabelle ---------------------------

def create_base_table(conn: sqlite3.Connection, table: str):
    if table_exists(conn, table):
        return
    ddl = (
        'CREATE TABLE "' + table + '" (\n'
        '    "id" INTEGER PRIMARY KEY AUTOINCREMENT,\n'
        '    "logseq" TEXT DEFAULT (hex(randomblob(5)))\n'
        ');'
    )
    conn.execute(ddl)
    conn.commit()

def create_table_with_columns(conn: sqlite3.Connection, table: str, cols_spec: List[Tuple[str, str, bool]], fks: List[Tuple[str, str, str]]):
    if table_exists(conn, table):
        raise RuntimeError(f"La tabella '{table}' esiste già.")

    col_defs = [
        '"id" INTEGER PRIMARY KEY AUTOINCREMENT',
        '"logseq" TEXT DEFAULT (hex(randomblob(5)))'
    ]
    for name, sql_type, default_zero in cols_spec:
        if default_zero:
            col_defs.append(f'"{name}" {sql_type} DEFAULT 0')
        else:
            col_defs.append(f'"{name}" {sql_type}')
    fk_defs = [f'FOREIGN KEY ("{c}") REFERENCES "{rt}"("{rc}")' for (c, rt, rc) in fks]
    ddl = f'CREATE TABLE "{table}" (\n  ' + ",\n  ".join(col_defs + fk_defs) + "\n);"
    conn.execute(ddl)
    conn.commit()

# --------------------------- FK utils ---------------------------

def detect_foreign_keys(cols_spec: List[Tuple[str, str, bool]]) -> List[Tuple[str, str, str]]:
    fks: List[Tuple[str, str, str]] = []
    for name, sql_type, default_zero in cols_spec:
        if name.endswith("_id") and len(name) > 3:
            fks.append((name, name[:-3], "id"))
    return fks

def ensure_fk_tables(conn: sqlite3.Connection, fk_list: List[Tuple[str, str, str]]):
    for (_col, ref_table, _ref_col) in fk_list:
        if not table_exists(conn, ref_table):
            print(f"[INFO] Creo tabella referenziata '{ref_table}' (schema base) ...")
            create_base_table(conn, ref_table)
            apply_triggers_to_table(conn, ref_table)

# --------------------------- AUTOINCREMENT helpers ---------------------------

def read_autoincrement_seq(conn: sqlite3.Connection, table: str) -> int | None:
    """Legge il valore corrente della sequenza AUTOINCREMENT per la tabella da sqlite_sequence."""
    try:
        row = conn.execute('SELECT seq FROM sqlite_sequence WHERE name=?', (table,)).fetchone()
        if row is None:
            return None
        return row[0] if isinstance(row, tuple) else row['seq']
    except Exception:
        return None


def restore_autoincrement_seq(conn: sqlite3.Connection, table: str, seq_value: Optional[int]) -> None:
    """Ripristina esattamente la seq precedente (se disponibile) nella sqlite_sequence."""
    if seq_value is None:
        return
    try:
        exists = conn.execute('SELECT 1 FROM sqlite_sequence WHERE name=?', (table,)).fetchone()
        if exists:
            conn.execute('UPDATE sqlite_sequence SET seq=? WHERE name=?', (seq_value, table))
        else:
            conn.execute('INSERT INTO sqlite_sequence(name, seq) VALUES(?, ?)', (table, seq_value))
    except sqlite3.OperationalError:
        # se sqlite_sequence non esiste ancora, ignora
        pass


def fetch_max_id(conn: sqlite3.Connection, table: str):
    # Deprecated: preserviamo la seq originale da sqlite_sequence, non MAX(id)
    return None



def restore_autoincrement_seq(conn: sqlite3.Connection, table: str, max_id: Optional[int]) -> None:
    if max_id is None:
        return
    try:
        exists = conn.execute('SELECT 1 FROM sqlite_sequence WHERE name=?', (table,)).fetchone()
        if exists:
            conn.execute('UPDATE sqlite_sequence SET seq=? WHERE name=?', (max_id, table))
        else:
            conn.execute('INSERT INTO sqlite_sequence(name, seq) VALUES(?, ?)', (table, max_id))
    except sqlite3.OperationalError:
        # sqlite_sequence may not exist if AUTOINCREMENT hasn't been created yet
        pass

# --------------------------- Rebuild (ricreazione tabella) ---------------------------

def generic_rebuild(conn: sqlite3.Connection, table: str, new_defs: List[str], fk_clause: List[str], copy_cols: List[str], idx_sqls: List[str]) -> None:
    """Generic rebuild primitive that preserves AUTOINCREMENT and re-applies indices and triggers.
    Other features should delegate to this to avoid code duplication."""
    tmp = f"{table}__tmp__rebuild"
    with conn:
        _prev_seq = read_autoincrement_seq(conn, table)
        conn.execute(f'DROP TABLE IF EXISTS "{tmp}"')
        try:
            ddl = f'CREATE TABLE "{tmp}" (\n  ' + ",\n  ".join(new_defs + fk_clause) + "\n);"
            conn.execute(ddl)

            col_list = ", ".join(f'"{c}"' for c in copy_cols)
            try:
                conn.execute(f'INSERT INTO "{tmp}" ({col_list}) SELECT {col_list} FROM "{table}"')
            except sqlite3.IntegrityError as e:
                print(f"[ERRORE] Inserimento dati nel tmp fallito: {e}")
                print(_fk_violations_report(conn, table))
                raise

            conn.execute(f'DROP TABLE "{table}"')
            conn.execute(f'ALTER TABLE "{tmp}" RENAME TO "{table}"')

            restore_autoincrement_seq(conn, table, _prev_seq)

            for sql in idx_sqls:
                try:
                    conn.execute(sql)
                except Exception as e:
                    print(f"[WARN] Ricreazione indice fallita, lo salto: {e}")

            apply_triggers_to_table(conn, table)
        finally:
            try:
                conn.execute(f'DROP TABLE IF EXISTS "{tmp}"')
            except Exception:
                pass



def _column_def_from_info(info: sqlite3.Row, override_type: str | None = None) -> str:
    """Costruisce il DDL di una colonna a partire dal risultato di PRAGMA table_info.
    - Mantiene NOT NULL e DEFAULT
    - Ignora vincoli PK (gestiamo 'id' a parte)
    - Consente override del tipo via override_type"""
    name = info[1] if isinstance(info, tuple) else info['name']
    ctype = (override_type or (info[2] if isinstance(info, tuple) else info['type']) or 'TEXT').strip()
    notnull = (info[3] if isinstance(info, tuple) else info['notnull'])
    dflt = (info[4] if isinstance(info, tuple) else info['dflt_value'])
    parts = [f'"{name}" {ctype}']
    if notnull:
        parts.append('NOT NULL')
    if dflt is not None:
        # dflt_value può essere già quotato o una funzione; non alterarlo
        parts.append(f'DEFAULT {dflt}')
    return ' '.join(parts)
def recreate_table_with_mapping(conn: sqlite3.Connection, table: str,
                                new_order: List[str],
                                type_overrides: Optional[Dict[str,str]]=None,
                                add_fk_cols: Optional[List[str]]=None):
    """Ricrea la tabella solo quando necessario (riordino/cambio tipo/aggiunta FK),
    salvando dati, indici, trigger e AUTOINCREMENT."""
    type_overrides = type_overrides or {}
    add_fk_cols = add_fk_cols or []

    cols_info = get_columns_info(conn, table)
    name_to_info = {c["name"]: c for c in cols_info if c["name"] not in ("id","logseq")}
    existing_other_cols = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
    if not new_order:
        new_order = existing_other_cols[:]

    for c in new_order:
        if c not in name_to_info:
            raise RuntimeError(f"Colonna inesistente: {c}")

    fk_existing = get_fks(conn, table)
    fk_defs = [(r["from"], r["table"], r["to"] or "id") for r in fk_existing]

    for col in add_fk_cols:
        if not col.endswith("_id") or col not in name_to_info:
            continue
        fk_defs.append((col, col[:-3], "id"))

    new_defs: List[str] = []
    new_defs.append('"id" INTEGER PRIMARY KEY AUTOINCREMENT')
    new_defs.append('"logseq" TEXT DEFAULT (hex(randomblob(5)))')
    for c in new_order:
        info = name_to_info[c]
        new_defs.append(_column_def_from_info(info, override_type=type_overrides.get(c)))

    fk_clause = [f'FOREIGN KEY ("{c}") REFERENCES "{rt}"("{rc}")' for (c,rt,rc) in fk_defs]

    idx_sqls = get_index_create_statements(conn, table)

    copy_cols = ["id","logseq"] + new_order
    generic_rebuild(conn, table, new_defs, fk_clause, copy_cols, idx_sqls)

# --------------------------- Menu: Aggiungi tabella ---------------------------

def menu_aggiungi_tabella():
    print_header("MENU — Aggiungi tabella")
    while True:
        table = ask("Nome tabella principale (<main>): ").strip()
        if table:
            break
        print("Nome tabella non valido. Riprova.")

    while True:
        prompt = """Specifica colonne: "nome tipo" con comma,
dove tipo in {i|r|t} + opzionale 'z' per default 0
(es. 'inizio t, fine t, esempio iz'):
> """
        raw_cols = ask(prompt).strip()
        try:
            cols_spec = parse_columns_spec(raw_cols)
            break
        except Exception as e:
            print(f"[ERRORE] {e}")
            print("Riprova l'inserimento delle colonne.\n")

    fks = detect_foreign_keys(cols_spec)

    with connect_db() as conn:
        try:
            ensure_fk_tables(conn, fks)
            print(f"[INFO] Creo tabella '{table}' ...")
            create_table_with_columns(conn, table, cols_spec, fks)
            apply_triggers_to_table(conn, table)
            print(f"[FATTO] Tabella '{table}' creata con successo.")
        except Exception as e:
            print(f"[ERRORE] Durante la creazione di '{table}': {e}")

# --------------------------- Menu: Modifica tabella ---------------------------

def _select_table_interactive(conn: sqlite3.Connection) -> Optional[str]:
    tables = list_tables(conn)
    if not tables:
        print("[INFO] Nessuna tabella trovata.")
        return None
    print_header("Seleziona tabella")
    for i, t in enumerate(tables, start=1):
        print(f"  {i}) {t}")
    choice = ask("> Numero tabella: ").strip()
    if not choice.isdigit():
        print("Scelta non valida.")
        return None
    idx = int(choice)
    if idx < 1 or idx > len(tables):
        print("Indice fuori range.")
        return None
    return tables[idx - 1]

def _rename_table_and_update_triggers(conn: sqlite3.Connection, old: str, new: str):
    with conn:
        conn.execute(f'ALTER TABLE "{old}" RENAME TO "{new}"')
        print(f"[OK] Tabella rinominata: {old} → {new}")
        apply_triggers_to_table(conn, new)

        # Aggiorna i trigger testuali nel DB
        rows = conn.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger' AND sql IS NOT NULL").fetchall()
        for r in rows:
            tname, tsql = r["name"], r["sql"]
            if not tsql:
                continue
            new_sql = tsql.replace(f'"{old}"', f'"{new}"')
            new_sql = new_sql.replace(f"'{old}'", f"'{new}'")
            new_sql = new_sql.replace(f"audit__{old}__", f"audit__{new}__")
            if new_sql != tsql:
                drop_trigger_if_exists(conn, tname)
                try:
                    conn.executescript(new_sql)
                    print(f"[OK] Trigger aggiornato: {tname}")
                except Exception as e:
                    print(f"[WARN] Impossibile aggiornare trigger {tname}: {e}")

def _rename_columns_simple(conn: sqlite3.Connection, table: str, mapping: List[Tuple[str,str]]):
    with conn:
        for old, new in mapping:
            print(f"[INFO] Rinomino colonna: {old} → {new}")
            conn.execute(f'ALTER TABLE "{table}" RENAME COLUMN "{old}" TO "{new}"')
        apply_triggers_to_table(conn, table)
    print("[FATTO] Rinomina colonne completata.")

def _fetch_unique_singlecol_indexes(conn: sqlite3.Connection, table: str) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    idxs = conn.execute(f"PRAGMA index_list('{table}')").fetchall()
    for r in idxs:
        if not r["unique"]:
            continue
        iname = r["name"]
        cols = conn.execute(f"PRAGMA index_info('{iname}')").fetchall()
        if len(cols) == 1:
            colname = cols[0]["name"]
            result.setdefault(colname, []).append(iname)
    return result

def _toggle_unique_indexes(conn: sqlite3.Connection, table: str, cols: List[str]):
    existing_uni = _fetch_unique_singlecol_indexes(conn, table)
    with conn:
        for c in cols:
            if c in ("id","logseq"):
                print(f"[SKIP] Non si può modificare UNIQUE su {c}.")
                continue
            if c in existing_uni and existing_uni[c]:
                for iname in existing_uni[c]:
                    print(f"[INFO] DROP UNIQUE INDEX {iname}")
                    conn.execute(f'DROP INDEX IF EXISTS "{iname}"')
                print(f"[FATTO] Rimosso UNIQUE da '{c}'.")
            else:
                iname = f'u__{table}__{c}'
                try:
                    conn.execute(f'CREATE UNIQUE INDEX "{iname}" ON "{table}" ("{c}")')
                    print(f"[FATTO] Aggiunto UNIQUE a '{c}' tramite indice {iname}.")
                except sqlite3.IntegrityError as e:
                    print(f"[ERRORE] Non posso rendere UNIQUE '{c}' su '{table}': {e}")
                    try:
                        print(_unique_violations_report(conn, table, [c]))
                    except Exception:
                        pass

def menu_modifica_tabella():
    print_header("MENU — Modifica tabella")
    # scegli tabella con una connessione effimera
    with connect_db() as conn_select:
        table = _select_table_interactive(conn_select)
    if not table:
        return

    # se modalità RIUSO, mantieni connessione aperta; altrimenti apri per-azione
    persistent_conn = connect_db() if not REOPEN_PER_ACTION else None

    try:
        while True:
            print_header(f"Modifica '{table}'   [Modalità connessione: {mode_label()}]")
            print("  1) Rinomina tabella")
            print("  2) Rinomina colonna/e (per numero)")
            print("  3) Cambia tipo colonna (per numero)")
            print("  4) Toggle UNIQUE su colonne (per numeri)")
            print("  5) Aggiungi FOREIGN KEY da colonne")
            print("  6) Cambia ordine colonne (esclude id/logseq)")
            print("  0) Indietro")
            choice = ask("> ").strip()

            if choice == "1":
                if REOPEN_PER_ACTION:
                    with connect_db() as conn:
                        nuovo = ask("Nuovo nome tabella: ").strip()
                        if not nuovo:
                            print("Nome non valido."); continue
                        try:
                            _rename_table_and_update_triggers(conn, table, nuovo)
                            table = nuovo
                        except Exception as e:
                            print(f"[ERRORE] Rinomina tabella: {e}")
                else:
                    conn = persistent_conn
                    nuovo = ask("Nuovo nome tabella: ").strip()
                    if not nuovo:
                        print("Nome non valido."); continue
                    try:
                        _rename_table_and_update_triggers(conn, table, nuovo)
                        table = nuovo
                    except Exception as e:
                        print(f"[ERRORE] Rinomina tabella: {e}")

            elif choice == "2":
                if REOPEN_PER_ACTION:
                    with connect_db() as conn:
                        current = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                        print("Colonne disponibili:")
                        for i, c in enumerate(current, 1):
                            print(f"  {i}) {c}")
                        raw = ask('Mappature "<num> nuovo_nome" separate da virgola (es: 1 mele, 4 banane):\n> ').strip()
                        if not raw: continue
                        pairs: List[Tuple[str,str]] = []
                        for part in raw.split(","):
                            p = part.strip()
                            if not p: continue
                            bits = p.split()
                            if len(bits) < 2 or not bits[0].isdigit():
                                print(f"[WARN] Mappatura non valida: {p} (serve: NUM NUOVO_NOME)")
                                continue
                            idx = int(bits[0])
                            if idx < 1 or idx > len(current):
                                print(f"[WARN] Indice fuori range: {idx}")
                                continue
                            old_name = current[idx-1]; new_name = bits[1]
                            pairs.append((old_name, new_name))
                        if not pairs: continue
                        try:
                            _rename_columns_simple(conn, table, pairs)
                        except Exception as e:
                            print(f"[ERRORE] Rinomina colonne: {e}")
                else:
                    conn = persistent_conn
                    current = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                    print("Colonne disponibili:")
                    for i, c in enumerate(current, 1):
                        print(f"  {i}) {c}")
                    raw = ask('Mappature "<num> nuovo_nome" separate da virgola (es: 1 mele, 4 banane):\n> ').strip()
                    if not raw: continue
                    pairs: List[Tuple[str,str]] = []
                    for part in raw.split(","):
                        p = part.strip()
                        if not p: continue
                        bits = p.split()
                        if len(bits) < 2 or not bits[0].isdigit():
                            print(f"[WARN] Mappatura non valida: {p} (serve: NUM NUOVO_NOME)")
                            continue
                        idx = int(bits[0])
                        if idx < 1 or idx > len(current):
                            print(f"[WARN] Indice fuori range: {idx}")
                            continue
                        old_name = current[idx-1]; new_name = bits[1]
                        pairs.append((old_name, new_name))
                    if not pairs: continue
                    try:
                        _rename_columns_simple(conn, table, pairs)
                    except Exception as e:
                        print(f"[ERRORE] Rinomina colonne: {e}")

            elif choice == "3":
                if REOPEN_PER_ACTION:
                    with connect_db() as conn:
                        current = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                        print("Colonne disponibili:")
                        for i, c in enumerate(current, 1):
                            print(f"  {i}) {c}")
                        raw = ask('Inserisci "<num> i/r/t" (es: 2 t):\n> ').strip()
                        m = re.match(r'^(\d+)\s+([irtIRT])$', raw)
                        if not m: print("Formato non valido."); continue
                        idx = int(m.group(1)); tcode = m.group(2).lower()
                        if idx < 1 or idx > len(current): print("Indice fuori range."); continue
                        col = current[idx-1]; sql_type = TYPE_MAP[tcode]
                        try:
                            order = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                            recreate_table_with_mapping(conn, table, order, type_overrides={col: sql_type})
                            print("==================================================")
                            print(f"[TABELLA RICREATA] {table}")
                            print("==================================================")
                        except Exception as e:
                            print(f"[ERRORE] Cambio tipo su '{table}': {e}")
                            try:
                                print(_fk_violations_report(conn, table))
                            except Exception:
                                pass
                else:
                    conn = persistent_conn
                    current = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                    print("Colonne disponibili:")
                    for i, c in enumerate(current, 1):
                        print(f"  {i}) {c}")
                    raw = ask('Inserisci "<num> i/r/t" (es: 2 t):\n> ').strip()
                    m = re.match(r'^(\d+)\s+([irtIRT])$', raw)
                    if not m: print("Formato non valido."); continue
                    idx = int(m.group(1)); tcode = m.group(2).lower()
                    if idx < 1 or idx > len(current): print("Indice fuori range."); continue
                    col = current[idx-1]; sql_type = TYPE_MAP[tcode]
                    try:
                        order = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                        recreate_table_with_mapping(conn, table, order, type_overrides={col: sql_type})
                        print("==================================================")
                        print(f"[TABELLA RICREATA] {table}")
                        print("==================================================")
                    except Exception as e:
                        print(f"[ERRORE] Cambio tipo su '{table}': {e}")
                        try:
                            print(_fk_violations_report(conn, table))
                        except Exception:
                            pass

            elif choice == "4":
                if REOPEN_PER_ACTION:
                    with connect_db() as conn:
                        current = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                        print("Colonne disponibili:")
                        for i, c in enumerate(current, 1):
                            print(f"  {i}) {c}")
                        raw = ask('Numeri colonne (comma-separated) su cui fare toggle UNIQUE (es: 1, 3, 5):\n> ').strip()
                        if not raw: continue
                        try:
                            idxs = [int(x.strip()) for x in raw.split(",") if x.strip()]
                        except Exception:
                            print("Input non valido."); continue
                        if any(i < 1 or i > len(current) for i in idxs):
                            print("Almeno un indice è fuori range."); continue
                        cols = [current[i-1] for i in idxs]
                        try:
                            _toggle_unique_indexes(conn, table, cols)
                        except Exception as e:
                            print(f"[ERRORE] Toggle UNIQUE: {e}")
                else:
                    conn = persistent_conn
                    current = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                    print("Colonne disponibili:")
                    for i, c in enumerate(current, 1):
                        print(f"  {i}) {c}")
                    raw = ask('Numeri colonne (comma-separated) su cui fare toggle UNIQUE (es: 1, 3, 5):\n> ').strip()
                    if not raw: continue
                    try:
                        idxs = [int(x.strip()) for x in raw.split(",") if x.strip()]
                    except Exception:
                        print("Input non valido."); continue
                    if any(i < 1 or i > len(current) for i in idxs):
                        print("Almeno un indice è fuori range."); continue
                    cols = [current[i-1] for i in idxs]
                    try:
                        _toggle_unique_indexes(conn, table, cols)
                    except Exception as e:
                        print(f"[ERRORE] Toggle UNIQUE: {e}")

            elif choice == "5":
                if REOPEN_PER_ACTION:
                    with connect_db() as conn:
                        raw = ask('Colonne da convertire in FOREIGN KEY (comma-separated, es: author_id, place_id):\n> ').strip()
                        cols = [c.strip() for c in raw.split(",") if c.strip()]
                        if not cols: continue
                        try:
                            need_fks = [(c, c[:-3], "id") for c in cols if c.endswith("_id")]
                            ensure_fk_tables(conn, need_fks)
                            order = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                            recreate_table_with_mapping(conn, table, order, add_fk_cols=cols)
                            print("==================================================")
                            print(f"[TABELLA RICREATA] {table}")
                            print("==================================================")
                        except Exception as e:
                            print(f"[ERRORE] Aggiunta FK su '{table}': {e}")
                            try:
                                print(_fk_violations_report(conn, table))
                            except Exception:
                                pass
                else:
                    conn = persistent_conn
                    raw = ask('Colonne da convertire in FOREIGN KEY (comma-separated, es: author_id, place_id):\n> ').strip()
                    cols = [c.strip() for c in raw.split(",") if c.strip()]
                    if not cols: continue
                    try:
                        need_fks = [(c, c[:-3], "id") for c in cols if c.endswith("_id")]
                        ensure_fk_tables(conn, need_fks)
                        order = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                        recreate_table_with_mapping(conn, table, order, add_fk_cols=cols)
                        print("==================================================")
                        print(f"[TABELLA RICREATA] {table}")
                        print("==================================================")
                    except Exception as e:
                        print(f"[ERRORE] Aggiunta FK su '{table}': {e}")
                        try:
                            print(_fk_violations_report(conn, table))
                        except Exception:
                            pass

            elif choice == "6":
                if REOPEN_PER_ACTION:
                    with connect_db() as conn:
                        current = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                        print("Ordine attuale:")
                        for i, c in enumerate(current, 1):
                            print(f"  {i}) {c}")
                        raw = ask("Nuovo ordine (lista di numeri separati da virgola):\n> ").strip()
                        try:
                            idxs = [int(x.strip()) for x in raw.split(",") if x.strip()]
                        except Exception:
                            print("Input non valido."); continue
                        if sorted(idxs) != list(range(1, len(current)+1)):
                            print("Gli indici devono essere una permutazione completa (1..N)."); continue
                        new_order = [current[i-1] for i in idxs]
                        try:
                            recreate_table_with_mapping(conn, table, new_order)
                            print("==================================================")
                            print(f"[TABELLA RICREATA] {table}")
                            print("==================================================")
                        except Exception as e:
                            print(f"[ERRORE] Riordino tabella '{table}': {e}")
                            try:
                                print(_fk_violations_report(conn, table))
                            except Exception:
                                pass
                else:
                    conn = persistent_conn
                    current = [c for c in get_columns(conn, table) if c not in ("id","logseq")]
                    print("Ordine attuale:")
                    for i, c in enumerate(current, 1):
                        print(f"  {i}) {c}")
                    raw = ask("Nuovo ordine (lista di numeri separati da virgola):\n> ").strip()
                    try:
                        idxs = [int(x.strip()) for x in raw.split(",") if x.strip()]
                    except Exception:
                        print("Input non valido."); continue
                    if sorted(idxs) != list(range(1, len(current)+1)):
                        print("Gli indici devono essere una permutazione completa (1..N)."); continue
                    new_order = [current[i-1] for i in idxs]
                    try:
                        recreate_table_with_mapping(conn, table, new_order)
                        print("==================================================")
                        print(f"[TABELLA RICREATA] {table}")
                        print("==================================================")
                    except Exception as e:
                        print(f"[ERRORE] Riordino tabella '{table}': {e}")
                        try:
                            print(_fk_violations_report(conn, table))
                        except Exception:
                            pass

            elif choice == "0":
                # torna al menu precedente (main)
                break
            else:
                print("Scelta non valida.")
    finally:
        if persistent_conn is not None:
            try:
                persistent_conn.close()
            except Exception:
                pass

# --------------------------- Memento cloud ---------------------------

def menu_memento_import_batch():
    print_header("MEMENTO — Importa batch da YAML")
    # DB_PATH è già definito nello script originale
    db_default = str(DB_PATH)
    db_path = ask(f"Percorso DB sqlite [{db_default}]: ").strip() or db_default
    batch_path = ask("Percorso batch YAML/INI [memento_import.ini]: ").strip() or "memento_import.ini"

    db_path_res = str(resolve_here(db_path))
    batch_path_res = str(resolve_here(batch_path))

    print(f"\n[INFO] DB: {db_path_res}\n       Batch: {batch_path_res}")
    try:
        n = memento_import_batch(db_path_res, batch_path_res)
        print(f"\n[OK] Import completato: {n} righe totali.")
    except Exception as e:
        print(f"\n[ERRORE] Import batch fallito: {e}")

def menu_memento():
    """Sottomenu Memento cloud (SDK legacy + nuovo import batch)."""
    while True:
        print_header("------ MEMENTO -------")
        print("1) Elenca librerie (SDK)")
        print("2) Deduci mappatura campi (SDK)")
        print("3) Mostra 1 entry grezza (SDK)")
        print("4) Importa libreria (auto)")
        print("5) Importa batch da YAML")
        print("0) Indietro")
        choice = ask("> ").strip()

        if choice == "0":
            break
        elif choice in ("1", "2", "3", "4"):
            print("Funzioni SDK legacy non disponibili in questa versione (usa l'opzione 5 per l'import batch).")
        elif choice == "5":
            menu_memento_import_batch()
        else:
            print("Scelta non valida.")


# --------------------------- Main ---------------------------

def main():
    global REOPEN_PER_ACTION

    _init_mode_from_config()
    sweep_stale_tmp_tables()

    print_header("CREA/MODIFICA TABELLE + TRIGGER (noutput.db) — versione completa")
    print(f"Percorso DB: {DB_PATH}")
    print(f"Template trigger: {TRIGGERS_PATH}\n")

    try:
        ver = tuple(int(x) for x in sqlite3.sqlite_version.split("."))
        if ver < (3, 31, 0):
            print("[ATTENZIONE] SQLite =", sqlite3.sqlite_version,
                  "— per usare DEFAULT (hex(randomblob(5))) serve >= 3.31.0. "
                  "Altrimenti riattiva il trigger set_logseq_random nel template.")
    except Exception:
        pass

    while True:
        print(f"[INFO] Modalità connessione attiva: {mode_label()}")
        print("Seleziona un'azione:")
        print("  1) Aggiungi tabella")
        print("  2) Modifica tabella")
        print("  3) Memento cloud")
        print(f"  4) Cambia modalità connessione (ora: {mode_label()})")
        print("  0) Esci")
        choice = ask("> ").strip()
        if choice == "1":
            menu_aggiungi_tabella()
        elif choice == "2":
            menu_modifica_tabella()
        elif choice == "3":
            menu_memento()
        elif choice == "4":
            REOPEN_PER_ACTION = not REOPEN_PER_ACTION
            cfg = load_config(); cfg["reopen_per_action"] = REOPEN_PER_ACTION; save_config(cfg)
            print(f"[OK] Modalità connessione impostata a: {mode_label()} (salvata in {CONFIG_PATH.name})")
        elif choice == "0":
            print("Uscita.")
            break
        else:
            print("Scelta non valida.")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("\n=== ERRORE NON GESTITO ===")
        print(f"{type(exc).__name__}: {exc}")
        import traceback
        traceback.print_exc()
        pause("\nPremi Invio per chiudere...")
        sys.exit(1)
    finally:
        pause()


def read_autoincrement_seq(conn: sqlite3.Connection, table: str) -> int | None:
    try:
        row = conn.execute('SELECT seq FROM sqlite_sequence WHERE name=?', (table,)).fetchone()
        if row is None:
            return None
        return row[0] if isinstance(row, tuple) else row['seq']
    except Exception:
        return None


def restore_autoincrement_seq(conn: sqlite3.Connection, table: str, seq_value: int | None) -> None:
    if seq_value is None:
        return
    try:
        exists = conn.execute('SELECT 1 FROM sqlite_sequence WHERE name=?', (table,)).fetchone()
        if exists:
            conn.execute('UPDATE sqlite_sequence SET seq=? WHERE name=?', (seq_value, table))
        else:
            conn.execute('INSERT INTO sqlite_sequence(name, seq) VALUES(?, ?)', (table, seq_value))
    except sqlite3.OperationalError:
        pass
