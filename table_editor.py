# -*- coding: utf-8 -*-
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List, Dict, Optional

from util import resolve_here


def _ask(prompt: str, default: str = "") -> str:
    raw = input(f"{prompt} [{default}]: ").strip()
    return raw or default


def _print_header(title: str) -> None:
    line = "=" * max(len(title), 40)
    print(f"\n{line}\n{title}\n{line}")


def _ask_db_path(default: str = "noutput.db") -> Path:
    db_str = _ask("Percorso DB sqlite", default)
    return resolve_here(db_str)


# ========================
#   UTIL SU METADATI
# ========================

def _list_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute(
        "SELECT name FROM sqlite_master "
        "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
        "ORDER BY 1"
    )
    return [row[0] for row in cur.fetchall()]


def _choose_table(conn: sqlite3.Connection) -> Optional[str]:
    tables = _list_tables(conn)
    if not tables:
        print("Nessuna tabella trovata nel database.")
        return None

    print("\nTabelle disponibili:")
    for i, name in enumerate(tables, start=1):
        print(f"  {i}) {name}")

    choice = _ask("Seleziona tabella (numero o nome)", "")
    if not choice:
        return None

    if choice.isdigit():
        idx = int(choice)
        if 1 <= idx <= len(tables):
            return tables[idx - 1]
        print("Indice fuori range.")
        return None

    if choice in tables:
        return choice

    print("Tabella non trovata.")
    return None


def _get_table_info(conn: sqlite3.Connection, table: str) -> List[sqlite3.Row]:
    cur = conn.execute(f'PRAGMA table_info("{table}")')
    return cur.fetchall()


def _get_fk_info(conn: sqlite3.Connection, table: str) -> List[sqlite3.Row]:
    cur = conn.execute(f'PRAGMA foreign_key_list("{table}")')
    return cur.fetchall()


def _build_fk_clauses(fks: List[sqlite3.Row]) -> List[str]:
    # Raggruppa per id di FK (stesso vincolo, potenzialmente multicolonna)
    grouped: Dict[int, List[sqlite3.Row]] = {}
    for r in fks:
        grouped.setdefault(r["id"], []).append(r)

    clauses: List[str] = []
    for _, rows in sorted(grouped.items()):
        base = rows[0]
        from_cols = ", ".join(f'"{r["from"]}"' for r in rows)
        to_cols = ", ".join(f'"{r["to"]}"' for r in rows)
        clause = f'FOREIGN KEY({from_cols}) REFERENCES "{base["table"]}" ({to_cols})'
        if base["on_update"] and base["on_update"].upper() != "NO ACTION":
            clause += f" ON UPDATE {base['on_update'].upper()}"
        if base["on_delete"] and base["on_delete"].upper() != "NO ACTION":
            clause += f" ON DELETE {base['on_delete'].upper()}"
        if base["match"] and base["match"].upper() != "NONE":
            clause += f" MATCH {base['match'].upper()}"
        clauses.append(clause)
    return clauses


def _read_autoincrement_seq(conn: sqlite3.Connection, table: str) -> Optional[int]:
    try:
        row = conn.execute(
            "SELECT seq FROM sqlite_sequence WHERE name=?",
            (table,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row:
        return None
    return int(row[0])


def _restore_autoincrement_seq(conn: sqlite3.Connection, table: str, seq_value: Optional[int]) -> None:
    if seq_value is None:
        return
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_sequence WHERE name=?",
            (table,),
        ).fetchone()
        if exists:
            conn.execute(
                "UPDATE sqlite_sequence SET seq=? WHERE name=?",
                (seq_value, table),
            )
        else:
            conn.execute(
                "INSERT INTO sqlite_sequence(name, seq) VALUES(?, ?)",
                (table, seq_value),
            )
    except sqlite3.OperationalError:
        # Nessuna sqlite_sequence
        pass


def _get_schema_objects(conn: sqlite3.Connection, table: str) -> List[sqlite3.Row]:
    # Indici e trigger definiti esplicitamente (sql NON NULL)
    cur = conn.execute(
        "SELECT type, name, tbl_name, sql "
        "FROM sqlite_master "
        "WHERE tbl_name=? AND type IN ('index','trigger') AND sql IS NOT NULL "
        "ORDER BY type, name",
        (table,),
    )
    return cur.fetchall()


def _recreate_schema_objects(conn: sqlite3.Connection, objects: List[sqlite3.Row]) -> None:
    for obj in objects:
        sql = obj["sql"]
        if not sql:
            continue
        try:
            conn.execute(sql)
        except Exception as e:
            print(f"[WARN] Impossibile ricreare {obj['type']} {obj['name']}: {e}")


# ========================
#   CREATE / DROP
# ========================

def _menu_crea_tabella(db_path: Path) -> None:
    _print_header("CREA TABELLA")
    name = _ask("Nome nuova tabella", "").strip()
    if not name:
        print("Nome non valido, operazione annullata.")
        return

    try:
        num_cols = int(_ask("Numero di colonne (escluso id)", "0"))
    except ValueError:
        print("Valore non valido, operazione annullata.")
        return

    cols: List[str] = []
    for i in range(num_cols):
        col_name = _ask(f"  Nome colonna {i+1}", f"col{i+1}").strip()
        col_type = _ask(
            f"  Tipo colonna {i+1} (INTEGER, REAL, TEXT, BLOB)",
            "TEXT",
        ).strip().upper() or "TEXT"
        if not col_name:
            print("  Nome colonna vuoto, salto.")
            continue
        if col_type not in ("INTEGER", "REAL", "TEXT", "BLOB"):
            col_type = "TEXT"
        cols.append(f'"{col_name}" {col_type}')

    ddl_cols = ", ".join(cols)
    if ddl_cols:
        ddl = f'CREATE TABLE IF NOT EXISTS "{name}" (id INTEGER PRIMARY KEY AUTOINCREMENT, {ddl_cols});'
    else:
        ddl = f'CREATE TABLE IF NOT EXISTS "{name}" (id INTEGER PRIMARY KEY AUTOINCREMENT);'

    print("\nSQL generato:")
    print(ddl)
    if _ask("Confermi creazione tabella? (s/n)", "s").lower() != "s":
        print("Operazione annullata.")
        return

    with sqlite3.connect(db_path) as conn:
        conn.execute(ddl)
        conn.commit()
    print(f"\n[OK] Tabella '{name}' creata (se non esisteva già).")


def _menu_elimina_tabella(db_path: Path) -> None:
    _print_header("ELIMINA TABELLA")
    with sqlite3.connect(db_path) as conn:
        table = _choose_table(conn)
        if not table:
            return
        conferma = _ask(
            f"Sei sicuro di voler DROPPARE la tabella '{table}'? (scrivi SI in maiuscolo)",
            "NO",
        )
        if conferma != "SI":
            print("Operazione annullata.")
            return
        conn.execute(f'DROP TABLE IF EXISTS "{table}";')
        conn.commit()
    print(f"\n[OK] Tabella '{table}' eliminata (se esisteva).")


# ========================
#   REBUILD AVANZATO
# ========================

def _build_column_def(col: sqlite3.Row) -> str:
    name = col["name"]
    ctype = col["type"] or "TEXT"
    parts = [f'"{name}"', ctype]
    if col["pk"]:
        parts.append("PRIMARY KEY")
    if col["notnull"]:
        parts.append("NOT NULL")
    if col["dflt_value"] is not None:
        parts.append(f"DEFAULT {col['dflt_value']}")
    return " ".join(parts)


def _rebuild_table(
    conn: sqlite3.Connection,
    table: str,
    new_cols: List[sqlite3.Row],
) -> None:
    tmp_table = f"{table}__tmp__rebuild"

    # Metadati originali
    cols_old = _get_table_info(conn, table)
    fks = _get_fk_info(conn, table)
    fk_clauses = _build_fk_clauses(fks)
    schema_objects = _get_schema_objects(conn, table)
    seq_value = _read_autoincrement_seq(conn, table)

    # Costruisci DDL nuovo
    col_defs = [_build_column_def(c) for c in new_cols]
    all_defs = col_defs + fk_clauses
    ddl = f'CREATE TABLE "{table}" (\n  ' + ", \n  ".join(all_defs) + "\n);"

    # Elenco colonne comuni (per copia dati)
    old_names = [c["name"] for c in cols_old]
    new_names = [c["name"] for c in new_cols]
    common = [n for n in new_names if n in old_names]
    if not common:
        raise RuntimeError("Nessuna colonna in comune tra vecchia e nuova definizione, impossibile copiare i dati.")

    col_list = ", ".join(f'"{n}"' for n in common)

    with conn:
        # Rinomina tabella originale
        conn.execute(f'ALTER TABLE "{table}" RENAME TO "{tmp_table}"')

        # Crea nuova tabella con il nome originale
        conn.execute(ddl)

        # Copia i dati sulle colonne in comune, nell'ordine nuovo
        insert_sql = (
            f'INSERT INTO "{table}" ({col_list}) '
            f'SELECT {col_list} FROM "{tmp_table}"'
        )
        conn.execute(insert_sql)

        # Droppa la tabella temporanea
        conn.execute(f'DROP TABLE "{tmp_table}"')

        # Ripristina seq AUTOINCREMENT se esiste
        _restore_autoincrement_seq(conn, table, seq_value)

        # Ricrea indici e trigger espliciti
        _recreate_schema_objects(conn, schema_objects)


# ========================
#   UNIQUE
# ========================

def _get_unique_singlecol_indexes(conn: sqlite3.Connection, table: str) -> Dict[str, List[str]]:
    result: Dict[str, List[str]] = {}
    cur = conn.execute(f'PRAGMA index_list("{table}")')
    for row in cur.fetchall():
        idx_name = row[1]
        is_unique = bool(row[2])
        if not is_unique:
            continue
        cur2 = conn.execute(f'PRAGMA index_info("{idx_name}")')
        cols = [r[2] for r in cur2.fetchall()]
        if len(cols) != 1:
            continue
        col = cols[0]
        result.setdefault(col, []).append(idx_name)
    return result


def _toggle_unique(conn: sqlite3.Connection, table: str, col: str) -> None:
    existing = _get_unique_singlecol_indexes(conn, table)
    if col in existing and existing[col]:
        with conn:
            for idx_name in existing[col]:
                print(f"[INFO] DROP UNIQUE INDEX {idx_name}")
                conn.execute(f'DROP INDEX IF EXISTS "{idx_name}"')
        print(f"[FATTO] Rimosso UNIQUE da '{col}'.")
        return

    idx_name = f"u__{table}__{col}"
    try:
        with conn:
            conn.execute(f'CREATE UNIQUE INDEX "{idx_name}" ON "{table}" ("{col}")')
        print(f"[FATTO] Aggiunto UNIQUE a '{col}' tramite indice {idx_name}.")
    except sqlite3.IntegrityError as e:
        print(f"[ERRORE] Non posso rendere UNIQUE '{col}' su '{table}': {e}")


# ========================
#   MENU MODIFICA
# ========================

def _rename_table(db_path: Path, table: str) -> str:
    new_name = _ask("Nuovo nome tabella", table).strip()
    if not new_name:
        print("Nome non valido, operazione annullata.")
        return table
    with sqlite3.connect(db_path) as conn:
        conn.execute(f'ALTER TABLE "{table}" RENAME TO "{new_name}"')
        conn.commit()
    print(f"[OK] Tabella rinominata in '{new_name}'.")
    return new_name


def _add_column(db_path: Path, table: str) -> None:
    col_name = _ask("Nome nuova colonna", "").strip()
    if not col_name:
        print("Nome non valido, operazione annullata.")
        return
    col_type = _ask("Tipo colonna (INTEGER, REAL, TEXT, BLOB)", "TEXT").strip().upper() or "TEXT"
    if col_type not in ("INTEGER", "REAL", "TEXT", "BLOB"):
        col_type = "TEXT"
    with sqlite3.connect(db_path) as conn:
        sql = f'ALTER TABLE "{table}" ADD COLUMN "{col_name}" {col_type};'
        print(f"\nSQL:\n{sql}")
        if _ask("Confermi aggiunta colonna? (s/n)", "s").lower() != "s":
            print("Operazione annullata.")
            return
        conn.execute(sql)
        conn.commit()
    print(f"[OK] Colonna '{col_name}' aggiunta alla tabella '{table}'.")


def _change_column_type(db_path: Path, table: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cols = _get_table_info(conn, table)
        if not cols:
            print("Nessuna colonna trovata.")
            return
        print("\nColonne disponibili:")
        for i, c in enumerate(cols, start=1):
            print(f"  {i}) {c['name']} ({c['type']})")

        choice = _ask("Seleziona colonna da modificare (numero)", "")
        if not choice.isdigit():
            print("Scelta non valida.")
            return
        idx = int(choice)
        if not (1 <= idx <= len(cols)):
            print("Indice fuori range.")
            return

        target = cols[idx - 1]
        new_type = _ask(
            f"Nuovo tipo per '{target['name']}'",
            target["type"] or "TEXT",
        ).strip().upper() or (target["type"] or "TEXT")

        if new_type not in ("INTEGER", "REAL", "TEXT", "BLOB"):
            print("Tipo non supportato in questa modalità.")
            return

        new_cols: List[sqlite3.Row] = []
        for c in cols:
            c2 = dict(c)
            if c["name"] == target["name"]:
                c2["type"] = new_type
            new_cols.append(c2)

        print("\nATTENZIONE: la tabella verrà ricostruita (DROP + CREATE + COPY dati) preservando indici, trigger e FK.")
        if _ask("Confermi operazione? (s/n)", "n").lower() != "s":
            print("Operazione annullata.")
            return

        _rebuild_table(conn, table, new_cols)  # type: ignore[arg-type]
        print(f"[OK] Tipo della colonna '{target['name']}' aggiornato a {new_type}.")


def _reorder_columns(db_path: Path, table: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cols = _get_table_info(conn, table)
        if not cols:
            print("Nessuna colonna trovata.")
            return
        print("\nOrdine attuale colonne:")
        for i, c in enumerate(cols, start=1):
            print(f"  {i}) {c['name']} ({c['type']})")

        raw = _ask(
            "Inserisci il nuovo ordine come lista di indici separati da virgola (es: 1,3,2,4)",
            ",".join(str(i) for i in range(1, len(cols) + 1)),
        )
        try:
            idxs = [int(x.strip()) for x in raw.split(",") if x.strip()]
        except ValueError:
            print("Formato non valido.")
            return
        if len(idxs) != len(cols) or sorted(idxs) != list(range(1, len(cols) + 1)):
            print("Gli indici devono essere una permutazione completa degli indici esistenti.")
            return

        new_cols: List[sqlite3.Row] = [dict(cols[i - 1]) for i in idxs]  # type: ignore[list-item]

        print("\nATTENZIONE: la tabella verrà ricostruita (DROP + CREATE + COPY dati) preservando indici, trigger e FK.")
        if _ask("Confermi operazione? (s/n)", "n").lower() != "s":
            print("Operazione annullata.")
            return

        _rebuild_table(conn, table, new_cols)  # type: ignore[arg-type]
        print("[OK] Ordine colonne aggiornato.")


def _toggle_unique_menu(db_path: Path, table: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cols = _get_table_info(conn, table)
        if not cols:
            print("Nessuna colonna trovata.")
            return
        print("\nColonne disponibili:")
        for i, c in enumerate(cols, start=1):
            print(f"  {i}) {c['name']} ({c['type']})")

        choice = _ask("Seleziona colonna per toggle UNIQUE (numero)", "")
        if not choice.isdigit():
            print("Scelta non valida.")
            return
        idx = int(choice)
        if not (1 <= idx <= len(cols)):
            print("Indice fuori range.")
            return
        col_name = cols[idx - 1]["name"]
        if col_name in ("id", "logseq"):
            print("Per sicurezza non modifico UNIQUE su 'id' o 'logseq'.")
            return

        _toggle_unique(conn, table, col_name)


def _menu_modifica_tabella(db_path: Path, table: str) -> str:
    """Ritorna il nome (potenzialmente rinominato) della tabella."""
    while True:
        _print_header(f"MODIFICA TABELLA — {table}")
        print("  1) Rinomina tabella")
        print("  2) Aggiungi colonna")
        print("  3) Cambia tipo colonna (con rebuild)")
        print("  4) Cambia ordine colonne (con rebuild)")
        print("  5) Toggle UNIQUE su colonna (via indici)")
        print("  0) Indietro")
        choice = _ask("Scelta", "0").strip()

        if choice == "0":
            break
        elif choice == "1":
            table = _rename_table(db_path, table)
        elif choice == "2":
            _add_column(db_path, table)
        elif choice == "3":
            _change_column_type(db_path, table)
        elif choice == "4":
            _reorder_columns(db_path, table)
        elif choice == "5":
            _toggle_unique_menu(db_path, table)
        else:
            print("Scelta non valida.")

    return table


# ========================
#   ENTRYPOINT MENU
# ========================

def menu_tabelle_sqlite() -> None:
    """Entry-point per il menu 'Gestione tabelle SQLite' chiamato dal menu principale."""
    db_path = _ask_db_path("noutput.db")
    while True:
        _print_header(f"GESTIONE TABELLE SQLITE — {db_path}")
        print("  1) Crea tabella")
        print("  2) Modifica tabella")
        print("  3) Elimina tabella")
        print("  0) Indietro / Esci")
        choice = _ask("Scelta", "0").strip()
        if choice == "1":
            _menu_crea_tabella(db_path)
        elif choice == "2":
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                table = _choose_table(conn)
            if table:
                _menu_modifica_tabella(db_path, table)
        elif choice == "3":
            _menu_elimina_tabella(db_path)
        elif choice == "0":
            break
        else:
            print("Scelta non valida.")
