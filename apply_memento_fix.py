# v4.0
"""Apply in-place compatibility fixes to scriptone's memento_sdk.py.

Adds/ensures at EOF:
- fetch_all_entries_full alias
- fetch_entry_detail alias
- fetch_incremental wrapper that:
  * accepts modified_after_iso
  * accepts positional args even if the underlying implementation is keyword-only
    by mapping positional args onto parameter names using inspect.signature.

Usage (from the scriptone folder):
    python apply_memento_fix.py
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

SDK_FILE = Path(__file__).with_name("memento_sdk.py")
BACKUP_FILE = Path(__file__).with_name("memento_sdk.py.bak")


def _has_def(src: str, name: str) -> bool:
    return re.search(rf"^\s*def\s+{re.escape(name)}\s*\(", src, flags=re.M) is not None


def main() -> None:
    if not SDK_FILE.exists():
        raise SystemExit(f"ERROR: {SDK_FILE} not found. Run this from your scriptone folder.")

    src = SDK_FILE.read_text(encoding="utf-8", errors="replace")

    # Backup once (do not overwrite an existing backup)
    if not BACKUP_FILE.exists():
        shutil.copyfile(SDK_FILE, BACKUP_FILE)

    additions = []

    if not _has_def(src, "fetch_all_entries_full"):
        additions.append(r'''
# --- compat: fetch_all_entries_full (alias) ---
def fetch_all_entries_full(*args, **kwargs):
    """Backwards-compatible alias for older callers."""
    for name in ("fetch_all_entries", "fetch_all_entries_paginated", "fetch_all_entries_all"):
        fn = globals().get(name)
        if callable(fn):
            return fn(*args, **kwargs)
    raise AttributeError(
        "fetch_all_entries_full alias could not find an underlying implementation. "
        "Expected one of: fetch_all_entries, fetch_all_entries_paginated, fetch_all_entries_all."
    )
''')

    if not _has_def(src, "fetch_entry_detail"):
        additions.append(r'''
# --- compat: fetch_entry_detail (alias) ---
def fetch_entry_detail(*args, **kwargs):
    """Backwards-compatible alias for older callers."""
    for name in ("fetch_entry_details", "fetch_entry_detail_full", "fetch_entry", "get_entry_detail", "get_entry"):
        fn = globals().get(name)
        if callable(fn):
            return fn(*args, **kwargs)
    raise AttributeError(
        "fetch_entry_detail alias could not find an underlying implementation. "
        "Expected one of: fetch_entry_details, fetch_entry_detail_full, fetch_entry, get_entry_detail, get_entry."
    )
''')

    # Always (re-)add a smarter wrapper; it will override earlier simplistic wrappers safely.
    additions.append(r'''
# --- compat: smart wrapper for fetch_incremental (modified_after_iso + positional args) ---
import inspect as _inspect

# Find the most "base" implementation we can wrap.
# If a previous compat wrapper stored _fetch_incremental_original, use that; otherwise use current fetch_incremental.
_base = globals().get("_fetch_incremental_original") or globals().get("_fetch_incremental_base") or globals().get("fetch_incremental")
globals()["_fetch_incremental_base"] = _base

def fetch_incremental(*args, modified_after_iso=None, **kwargs):  # type: ignore[override]
    """Compat wrapper.

    - Accepts modified_after_iso
    - Accepts positional args even if underlying implementation is keyword-only, by mapping them to parameter names.
    """
    fn = globals().get("_fetch_incremental_base") or _base
    if fn is None:
        raise AttributeError("fetch_incremental base implementation not found")

    try:
        sig = _inspect.signature(fn)
        params = [p for p in sig.parameters.values() if p.kind not in (_inspect.Parameter.VAR_POSITIONAL, _inspect.Parameter.VAR_KEYWORD)]
        # Map positional args onto parameter names in order, even if they are keyword-only.
        for i, val in enumerate(args):
            if i >= len(params):
                # Too many args; let the underlying function raise a clearer error
                break
            name = params[i].name
            if name not in kwargs:
                kwargs[name] = val
        args = ()  # always call keyword-only to satisfy keyword-only bases
        # Map modified_after_iso to whatever the base accepts
        if modified_after_iso is not None:
            if "modified_after" in sig.parameters and "modified_after" not in kwargs:
                kwargs["modified_after"] = modified_after_iso
            elif "modified_after_iso" in sig.parameters and "modified_after_iso" not in kwargs:
                kwargs["modified_after_iso"] = modified_after_iso
            elif "modified_after" not in kwargs:
                # last resort: keep it in kwargs under modified_after (most common)
                kwargs["modified_after"] = modified_after_iso
    except Exception:
        # If signature introspection fails, just do best-effort mapping
        if modified_after_iso is not None and "modified_after" not in kwargs and "modified_after_iso" not in kwargs:
            kwargs["modified_after"] = modified_after_iso

    return fn(*args, **kwargs)
''')

    patched = src.rstrip() + "\n\n" + "\n".join(additions).lstrip() + "\n"
    SDK_FILE.write_text(patched, encoding="utf-8")
    print("OK: Applied fixes to memento_sdk.py")
    print(f"Backup saved as: {BACKUP_FILE.name}")


if __name__ == "__main__":
    main()
