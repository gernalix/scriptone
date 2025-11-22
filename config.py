import os
import configparser
from typing import Any, Dict

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None

_DEFAULTS = {
    "database.path": "noutput.db",
    "memento.mode": "cloud",
    "memento.api_url": "https://api.mementodatabase.com/v1",
    "memento.token": "",
    "memento.timeout": 20,
    "memento.yaml_batch_path": "memento_import.yaml",
    "memento.ini_batch_path": "memento_import.ini",
}

def _flatten_ini(cfg: configparser.ConfigParser) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for section in cfg.sections():
        for k, v in cfg.items(section):
            flat[f"{section}.{k}"] = v
    return flat

def _flatten_yaml(d: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in d.items():
        key = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
        if isinstance(v, dict):
            out.update(_flatten_yaml(v, key))
        else:
            out[key] = v
    return out

def load_settings() -> Dict[str, Any]:
    settings: Dict[str, Any] = dict(_DEFAULTS)

    if os.path.exists("settings.ini"):
        cfg = configparser.ConfigParser()
        cfg.read("settings.ini", encoding="utf-8")
        settings.update(_flatten_ini(cfg))

    if os.path.exists("settings.yaml") and yaml is not None:
        with open("settings.yaml", "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        if isinstance(data, dict):
            settings.update(_flatten_yaml(data))

    for env_key, val in os.environ.items():
        low = env_key.lower()
        if low in ("database.path", "memento.mode", "memento.api_url",
                   "memento.token", "memento.timeout",
                   "memento.yaml_batch_path", "memento.ini_batch_path"):
            settings[low] = val

    try:
        settings["memento.timeout"] = int(settings.get("memento.timeout", 20))
    except Exception:
        settings["memento.timeout"] = 20

    return settings

def get(key: str, default: Any = None) -> Any:
    return load_settings().get(key, default)
