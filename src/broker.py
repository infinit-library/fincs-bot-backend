import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from src import config as runtime_config
from .config.setting import SaxoSettings
from .auth.saxo_oauth import SaxoOAuthClient


@dataclass
class BrokerResult:
    ok: bool
    order_id: Optional[str]
    error: Optional[str]
    payload: Optional[Dict[str, Any]]


def _load_uic_map(settings: dict) -> Dict[str, int]:
    raw = settings.get("saxo_uic_map") or {}
    if not isinstance(raw, dict):
        return {}
    mapped: Dict[str, int] = {}
    for key, val in raw.items():
        if key is None:
            continue
        try:
            mapped[str(key).upper()] = int(val)
        except (TypeError, ValueError):
            continue
    return mapped


def get_broker(name: str):
    name = (name or "").lower()
    if name in ("saxo", "", None):
        settings = SaxoSettings.from_env()
        oauth = SaxoOAuthClient(settings)
        account_key = os.getenv("SAXO_ACCOUNT_KEY")
        uic_map = _load_uic_map(runtime_config.load_settings())
        from .brokers.saxo import SaxoBroker

        return SaxoBroker(oauth, settings, account_key=account_key, uic_map=uic_map)
    raise ValueError(f"Unsupported broker: {name}")
