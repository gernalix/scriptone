# memento_sdk.py — list endpoints now request full fields in one go.
# Minimal, conservative patch: adds params.setdefault("include","fields") in list calls.
# If your API uses a different keyword, uncomment "expand" or "full" lines below.

from typing import Dict, Any, List, Optional, Callable
import time
import json
import os
import re

import requests
import certifi

for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "CURL_CA_BUNDLE"):
    os.environ.pop(var, None)

os.environ["SSL_CERT_FILE"] = certifi.where()


# -----------------------
# Config helpers
# -----------------------

def _cfg_get(key: str, default=None):
    """
    Read settings from environment or from settings.yaml/.yml/.ini (if present).
    Looks for keys under any section; accepts both 'memento.token' and 'token' naming.
    """
    try:
        import configparser, yaml  # type: ignore
    except Exception:
        configparser = None
        yaml = None

    env_map = {
        "memento.token": "MEMENTO_TOKEN",
        "memento.api_url": "MEMENTO_API_URL",
        "memento.timeout": "MEMENTO_TIMEOUT",
    }
    if key in env_map and env_map[key] in os.environ:
        return os.environ[env_map[key]]

    for fn in ("settings.yaml", "settings.yml", "settings.ini"):
        if not os.path.exists(fn):
            continue
        try:
            if fn.endswith(".ini") and configparser:
                cp = configparser.ConfigParser()
                cp.read(fn, encoding="utf-8")
                # try exact section.key match, else scan all sections for the last token
                if "." in key:
                    sect, opt = key.split(".", 1)
                    if cp.has_section(sect) and cp.has_option(sect, opt):
                        return cp.get(sect, opt)
                opt = key.split(".")[-1]
                for sect in cp.sections():
                    if cp.has_option(sect, opt):
                        return cp.get(sect, opt)
            elif yaml:
                with open(fn, "r", encoding="utf-8") as fh:
                    y = yaml.safe_load(fh) or {}
                if isinstance(y, dict):
                    # try deep traversal by last token
                    opt = key.split(".")[-1]
                    for sect, vals in y.items():
                        if isinstance(vals, dict) and opt in vals:
                            return vals[opt]
                    # also allow top-level direct key
                    if key in y:
                        return y[key]
        except Exception:
            continue
    return default

def _sanitize_url(url: str) -> str:
    if not url:
        return url
    url = re.split(r"[;#]", str(url), maxsplit=1)[0].strip()
    return re.sub(r"\s+", "", url)

def _base_url() -> str:
    # Default Memento Cloud v1; adjust if your account uses a different base
    u = _cfg_get("memento.api_url", "https://api.mementodatabase.com/v1")
    return _sanitize_url(u or "")

def _timeout() -> int:
    try:
        return int(_cfg_get("memento.timeout", 20))
    except Exception:
        return 20

def _token_params() -> Dict[str, Any]:
    token = (_cfg_get("memento.token", "") or "").strip()
    return {"token": token} if token else {}

# -----------------------
# HTTP with gentle backoff
# -----------------------

def _get_with_backoff(url, *, params=None, timeout=None, max_tries=12, base_sleep=1, max_sleep=30.0):
    import random
    tries = 0
    last_exc = None
    while tries < max_tries:
        try:
            r = requests.get(url, params=params or {}, timeout=timeout or _timeout())
            # Return on success or common client errors (let caller handle them)
            if r.status_code < 400 or r.status_code in (400, 401, 403, 404):
                return r
            # Respect rate limits / transient server errors
            if r.status_code == 429 or 500 <= r.status_code < 600:
                retry_after = r.headers.get("Retry-After")
                if retry_after:
                    try:
                        sleep_s = float(retry_after)
                    except Exception:
                        sleep_s = None
                else:
                    sleep_s = min(max_sleep, base_sleep * (2 ** tries)) + random.uniform(0, 0.4)
                time.sleep(sleep_s or 1.0)
                tries += 1
                continue
            return r
        except requests.RequestException as ex:
            last_exc = ex
            time.sleep(min(max_sleep, base_sleep * (2 ** tries)))
            tries += 1
            continue
    if last_exc:
        raise last_exc
    return r

def _raise_on_404(r, where: str):
    if r.status_code == 404:
        raise RuntimeError(f"Memento API 404 su {where}: {r.text}")
    r.raise_for_status()

# -----------------------
# Public API
# -----------------------

def list_libraries() -> List[Dict[str, Any]]:
    base = _base_url().rstrip("/")
    url = f"{base}/libraries"
    r = _get_with_backoff(url, params=_token_params(), timeout=_timeout())
    _raise_on_404(r, "/libraries")
    data = r.json()
    libs = data.get("libraries") or data.get("items") or data
    if isinstance(libs, dict):
        libs = list(libs.values())
    out = []
    for it in libs or []:
        out.append({
            "id": it.get("id") or it.get("library_id") or it.get("uuid"),
            "name": it.get("name") or it.get("title"),
            "title": it.get("title") or it.get("name"),
        })
    return out

def fetch_all_entries_full(library_id: str, limit: int = 100, progress: Optional[Callable[[Dict[str, Any]], None]] = None) -> List[Dict[str, Any]]:
    """
    Fetches all entries of a library, requesting full fields directly from the LIST endpoint.
    """
    base = _base_url().rstrip("/")
    url = f"{base}/libraries/{library_id}/entries"

    params = _token_params().copy()
    params["limit"] = int(limit)
    # >>> Patch: ask the API to include fields directly in the list response
    params.setdefault("include", "fields")
    # If your API ignores 'include', try one of the following (one at a time):
    # params.setdefault("expand", "fields")
    # params.setdefault("full", "1")

    all_rows: List[Dict[str, Any]] = []
    page = 0
    while True:
        t0 = time.perf_counter()
        r = _get_with_backoff(url, params=params, timeout=_timeout())
        dt = round(time.perf_counter() - t0, 3)
        _raise_on_404(r, url)
        data = r.json()
        rows = data.get("entries") or data.get("items") or data.get("data") or []
        if isinstance(rows, dict):
            rows = list(rows.values())
        all_rows.extend(rows)
        page += 1
        if progress:
            progress({"event": "page", "library_id": library_id, "page": page, "rows": len(rows), "sec": dt})
        token = data.get("nextPageToken") or data.get("cursor")
        if token:
            params = _token_params().copy()
            params["limit"] = int(limit)
            params.setdefault("include", "fields")  # keep asking for fields
            # params.setdefault("expand", "fields")
            # params.setdefault("full", "1")
            params["pageToken"] = token
            continue
        break
    return all_rows

def fetch_incremental(library_id: str, *, modified_after_iso: Optional[str], limit: int = 200, progress: Optional[Callable[[Dict[str, Any]], None]] = None) -> List[Dict[str, Any]]:
    """
    Incremental fetch using updatedAfter/modifiedAfter if available, requesting full fields from LIST.
    """
    base = _base_url().rstrip("/")
    url = f"{base}/libraries/{library_id}/entries"

    params = _token_params().copy()
    params["limit"] = int(limit)
    if modified_after_iso:
        # Some APIs use 'updatedAfter', others 'modifiedAfter'
        params["updatedAfter"] = modified_after_iso

    # >>> Patch: request fields inline
    params.setdefault("include", "fields")
    # params.setdefault("expand", "fields")
    # params.setdefault("full", "1")

    rows: List[Dict[str, Any]] = []
    page = 0
    while True:
        t0 = time.perf_counter()
        r = _get_with_backoff(url, params=params, timeout=_timeout())
        dt = round(time.perf_counter() - t0, 3)
        _raise_on_404(r, url)
        data = r.json()
        chunk = data.get("entries") or data.get("items") or data.get("data") or []
        if isinstance(chunk, dict):
            chunk = list(chunk.values())
        rows.extend(chunk)
        page += 1
        if progress:
            ev = {"event": "page", "library_id": library_id, "page": page, "rows": len(chunk), "sec": dt}
            if modified_after_iso:
                ev["updatedAfter"] = modified_after_iso
            progress(ev)
        time.sleep( float(_cfg_get("memento.page_delay_s", 1)) )
        token = data.get("nextPageToken") or data.get("cursor")
        if token:
            params = _token_params().copy()
            params["limit"] = int(limit)
            if modified_after_iso:
                params["updatedAfter"] = modified_after_iso
            params.setdefault("include", "fields")
            # params.setdefault("expand", "fields")
            # params.setdefault("full", "1")
            params["pageToken"] = token
            continue
        break
    return rows

def fetch_entry_detail(library_id: str, entry_id: str) -> Optional[Dict[str, Any]]:
    """
    Per-entry detail (kept for compatibility / fallbacks). Prefer list calls with include=fields.
    """
    base = _base_url().rstrip("/")
    url = f"{base}/libraries/{library_id}/entries/{entry_id}"
    r = _get_with_backoff(url, params=_token_params(), timeout=_timeout())
    if r.status_code == 404:
        return None
    _raise_on_404(r, url)
    try:
        return r.json()
    except Exception:
        return None
