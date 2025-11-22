"""
check_memento_api.py — Scansiona automaticamente tutte le librerie Memento
e verifica le capacità di sync incrementale (updatedAfter, createdAfter, ecc.)

Uso:
  python check_memento_api.py
  oppure
  python check_memento_api.py memento_import.ini
  python check_memento_api.py memento_import.yaml
"""
import sys, os, json, configparser
from typing import Dict, Any
from memento_sdk import probe_capabilities, list_libraries

def load_sections(path: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    if not os.path.exists(path):
        return out
    if path.lower().endswith(".ini"):
        cp = configparser.ConfigParser()
        cp.read(path, encoding="utf-8")
        for s in cp.sections():
            out[s] = {k: v for k, v in cp.items(s)}
    else:
        try:
            import yaml  # type: ignore
        except Exception:
            yaml = None
        if not yaml:
            return out
        with open(path, "r", encoding="utf-8") as fh:
            y = yaml.safe_load(fh) or {}
        for s, d in (y or {}).items():
            if isinstance(d, dict):
                out[s] = {k: str(v) for k, v in d.items()}
    return out

def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else None
    sections = load_sections(cfg_path) if cfg_path else {}
    ids_from_file = [
        v.get("library_id") or v.get("id")
        for v in sections.values()
        if isinstance(v, dict)
    ]
    ids_from_file = [x for x in ids_from_file if x]

    if ids_from_file:
        print(f"Trovate {len(ids_from_file)} librerie dal file di config.")
        lib_ids = ids_from_file
    else:
        print("Nessuna libreria nel file: eseguo scansione completa via API...")
        libs = list_libraries()
        lib_ids = [l["id"] for l in libs if l.get("id")]
        print(f"Trovate {len(lib_ids)} librerie totali.")

    report = {}
    for lib_id in lib_ids:
        try:
            caps = probe_capabilities(lib_id)
            strategy = (
                "updatedAfter"
                if caps.get("accepts_updatedAfter")
                else (
                    "createdAfter"
                    if caps.get("accepts_createdAfter")
                    else "paged-scan"
                )
            )
            report[lib_id] = {
                "caps": caps,
                "suggested_strategy": strategy,
            }
            print(f"[OK] {lib_id}: {strategy}")
        except Exception as e:
            report[lib_id] = {"error": str(e)}
            print(f"[ERRORE] {lib_id}: {e}")

    print("\n=== REPORT COMPLETO ===")
    print(json.dumps(report, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
