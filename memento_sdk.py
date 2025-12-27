# v2
# memento_sdk.py — robust config + token handling + backward-compatible API for scriptone

from __future__ import annotations

from typing import Dict, Any, List, Optional, Callable
import time
import json
import os
import re

import requests
import certifi

# ---------------------------------------------------------------------
# SSL env cleanup (kept from your original file)
# ---------------------------------------------------------------------
for var in ("REQUESTS_CA_BUNDLE", "SSL_CERT_FILE", "CURL_CA_BUNDLE"):
    os.environ.pop(var, None)

# ---------------------------------------------------------------------
# Base dir: ALWAYS the folder containing this file (fixes double-click / CWD issues)
# ---------------------------------------------------------------------
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ---------------------------------------------------------------------
# Config reading
# ---------------------------------------------------------------------
def _cfg_get(key: str, default=None):
    """
    Read settings from environment or from settings.yaml/.yml/.ini in the SAME folder as this script.

    Keys are expected as 'section.option' (e.g., 'memento.token').
    For INI:
      [memento]
      token = ...
    Also supports scanning other sections for last 'token' if needed.

    ENV mapping supported:
      MEMENTO_TOKEN, MEMENTO_API_URL, MEMENTO_TIMEOUT
    """
    # 1) ENV (only if non-empty)
    env_map = {
        "memento.token": "MEMENTO_TOKEN",
        "memento.api_url": "MEMENTO_API_URL",
        "memento.timeout": "MEMENTO_TIMEOUT",
    }
    if key in env_map:
        ev = (os.environ.get(env_map[key]) or "").strip()
        if ev:
            return ev

    # 2) Import config libs safely (do NOT couple configparser with yaml)
    try:
        import configparser
    except Exception:
        configparser = None

    try:
        import yaml  # type: ignore
    except Exception:
        yaml = None

    # 3) Read settings from script folder (robust)
    for fn in ("settings.yaml", "settings.yml", "settings.ini"):
        path = os.path.join(_BASE_DIR, fn)
        if not os.path.exists(path):
            continue
        try:
            if fn.endswith(".ini") and configparser:
                cp = configparser.ConfigParser()
                cp.read(path, encoding="utf-8")

                # exact section.option
                if "." in key:
                    sect, opt = key.split(".", 1)
                    if cp.has_section(sect) and cp.has_option(sect, opt):
                        return cp.get(sect, opt)

                # fallback: scan all sections for opt name
                opt_name = key.split(".", 1)[-1]
                last = None
                for sect in cp.sections():
                    if cp.has_option(sect, opt_name):
                        last = cp.get(sect, opt_name)
                if last is not None:
                    return last

            elif (fn.endswith(".yaml") or fn.endswith(".yml")) and yaml:
                with open(path, "r", encoding="utf-8") as f:
                    y = yaml.safe_load(f) or {}

                # nested section dict
                if "." in key:
                    sect, opt = key.split(".", 1)
                    if isinstance(y, dict) and isinstance(y.get(sect), dict) and opt in y[sect]:
                        return y[sect][opt]

                # direct key
                if isinstance(y, dict) and key in y:
                    return y[key]
        except Exception:
            # ignore bad config files
            continue

    return default


def _sanitize_url(url: str) -> str:
    if not url:
        return url
    url = re.split(r"[;#]", str(url), maxsplit=1)[0].strip()
    return re.sub(r"\s+", "", url)


def _base_url() -> str:
    # Default Memento Cloud v1
    return _sanitize_url(_cfg_get("memento.api_url", "https://api.mementodatabase.com/v1"))


def _timeout() -> float:
    try:
        return float(_cfg_get("memento.timeout", 20))
    except Exception:
        return 20.0


def _get_token_required() -> str:
    token = (_cfg_get("memento.token", "") or "").strip()
    if not token:
        raise RuntimeError(
            "MEMENTO token mancante. Metti in settings.ini:\n"
            "[memento]\n"
            "token = <IL_TUO_TOKEN>\n"
            f"(File letto da: {_BASE_DIR}\\settings.ini)"
        )
    return token


def _token_params() -> Dict[str, Any]:
    # ALWAYS return token (or raise)
    return {"token": _get_token_required()}


# ---------------------------------------------------------------------
# HTTP with gentle backoff
# ---------------------------------------------------------------------
def _get_with_backoff(url, *, params=None, timeout=None, max_tries=12, base_sleep=1, max_sleep=30.0):
    import random
    tries = 0
    last_exc = None
    while tries < max_tries:
        try:
            r = requests.get(
                url,
                params=params or {},
                timeout=timeout or _timeout(),
                verify=certifi.where(),
            )
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
        except Exception as e:
            last_exc = e
            time.sleep(min(max_sleep, base_sleep * (2 ** tries)))
            tries += 1

    if last_exc:
        raise last_exc
    raise RuntimeError("HTTP request failed (unknown)")


def _raise_on_404(r, where: str):
    if r.status_code == 404:
        raise RuntimeError(f"Memento API 404 su {where}: {r.text}")
    r.raise_for_status()


# ---------------------------------------------------------------------
# Public API (used by scriptone)
# ---------------------------------------------------------------------
def list_libraries() -> List[Dict[str, Any]]:
    base = _base_url().rstrip("/")
    url = f"{base}/libraries"
    params = _token_params().copy()
    r = _get_with_backoff(url, params=params)
    _raise_on_404(r, "list_libraries")
    return r.json()


def fetch_all_entries_full(
    library_id: str,
    *,
    limit: int = 100,
    progress: Optional[Callable[[Dict[str, Any]], None]] = None,
    max_pages: int = 0,
) -> List[Dict[str, Any]]:
    """
    Fetch ALL entries in a library, paging until done.
    Keeps include=fields so the caller gets full field payloads.
    """
    base = _base_url().rstrip("/")
    url = f"{base}/libraries/{library_id}/entries"

    all_rows: List[Dict[str, Any]] = []
    page = 0

    params = _token_params().copy()
    params["limit"] = int(limit)
    params.setdefault("include", "fields")

    while True:
        if max_pages and page >= max_pages:
            break

        t0 = time.time()
        r = _get_with_backoff(url, params=params)
        _raise_on_404(r, f"fetch_all_entries_full({library_id})")
        data = r.json()
        dt = round(time.time() - t0, 3)

        rows = data.get("entries") or data.get("data") or []
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
            params.setdefault("include", "fields")
            params["pageToken"] = token
            continue
        break

    return all_rows


def fetch_incremental(
    library_id: str,
    *,
    updated_after: str,
    limit: int = 100,
    progress: Optional[Callable[[Dict[str, Any]], None]] = None,
    probe: int = 0,
    max_pages: int = 0,
) -> List[Dict[str, Any]]:
    """
    Fetch entries updated after a given timestamp (incremental sync).
    Uses updatedAfter + include=fields, paging until done.
    """
    base = _base_url().rstrip("/")
    url = f"{base}/libraries/{library_id}/entries"

    all_rows: List[Dict[str, Any]] = []
    page = 0

    params = _token_params().copy()
    params["limit"] = int(limit)
    params["updatedAfter"] = updated_after
    params.setdefault("include", "fields")

    # optional small probe
    if probe and probe > 0:
        params_probe = dict(params)
        params_probe["limit"] = int(min(limit, probe))
        r_probe = _get_with_backoff(url, params=params_probe)
        _raise_on_404(r_probe, f"fetch_incremental.probe({library_id})")
        # don't early return; probe is just to test the endpoint quickly

    while True:
        if max_pages and page >= max_pages:
            break

        t0 = time.time()
        r = _get_with_backoff(url, params=params)
        _raise_on_404(r, f"fetch_incremental({library_id})")
        data = r.json()
        dt = round(time.time() - t0, 3)

        rows = data.get("entries") or data.get("data") or []
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
            params["updatedAfter"] = updated_after
            params.setdefault("include", "fields")
            params["pageToken"] = token
            continue
        break

    return all_rows


def fetch_entry_detail(library_id: str, entry_id: str) -> Dict[str, Any]:
    """
    Fetch a single entry detail. Some accounts expose /entries/<id>.
    """
    base = _base_url().rstrip("/")
    url = f"{base}/libraries/{library_id}/entries/{entry_id}"

    params = _token_params().copy()
    params.setdefault("include", "fields")

    r = _get_with_backoff(url, params=params)
    _raise_on_404(r, f"fetch_entry_detail({library_id},{entry_id})")
    return r.json()


def probe_capabilities(library_id: str) -> Dict[str, Any]:
    """
    Best-effort probe: checks which incremental params are accepted.
    Returns booleans for updatedAfter/createdAfter and whether include=fields works.
    """
    base = _base_url().rstrip("/")
    url = f"{base}/libraries/{library_id}/entries"

    out: Dict[str, Any] = {
        "updatedAfter": False,
        "createdAfter": False,
        "include_fields": False,
    }

    # include=fields
    try:
        p = _token_params().copy()
        p["limit"] = 1
        p["include"] = "fields"
        r = _get_with_backoff(url, params=p, max_tries=3)
        if r.status_code < 400:
            out["include_fields"] = True
    except Exception:
        pass

    # updatedAfter (use an ancient timestamp)
    try:
        p = _token_params().copy()
        p["limit"] = 1
        p["updatedAfter"] = "1970-01-01T00:00:00.000Z"
        p.setdefault("include", "fields")
        r = _get_with_backoff(url, params=p, max_tries=3)
        if r.status_code < 400:
            out["updatedAfter"] = True
    except Exception:
        pass

    # createdAfter (some APIs support it)
    try:
        p = _token_params().copy()
        p["limit"] = 1
        p["createdAfter"] = "1970-01-01T00:00:00.000Z"
        p.setdefault("include", "fields")
        r = _get_with_backoff(url, params=p, max_tries=3)
        if r.status_code < 400:
            out["createdAfter"] = True
    except Exception:
        pass

    return out
