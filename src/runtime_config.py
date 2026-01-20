from pathlib import Path
from typing import Any, Dict

BASE_DIR = Path(__file__).resolve().parent.parent
SETTINGS_PATH = BASE_DIR / "data" / "settings.json"

DEFAULT_SETTINGS: Dict[str, Any] = {
    "poll_interval": 15,
    "allowed_pairs": ["USDJPY", "EURUSD"],
    "max_lot_cap": 0.8,
    "dedup_window": 30,
    "signal_freshness_seconds": 180,
    "dry_run": True,
    "running": False,
    "strict_mode": True,
    "allow_market_without_prices": False,
    "process_last_n": 0,
    "base_units_per_lot": 100000,  # FX standard lot size
    "broker": "saxo",
    "max_slippage": 0.0005,  # 0.5 pip for most majors
    "max_spread": 0.0008,  # 0.8 pip guard
    "price_retries": 2,
    "conflict_policy": "skip",  # skip | close_then_open
    "max_open_positions": 5,
    "max_total_units": 500000,
    "headless_scrape": True,
    "saxo_uic_map": {},
}


def load_settings() -> Dict[str, Any]:
    if SETTINGS_PATH.exists():
        import json

        return json.loads(SETTINGS_PATH.read_text(encoding="utf-8"))
    return DEFAULT_SETTINGS.copy()


def save_settings(data: Dict[str, Any]) -> Dict[str, Any]:
    settings = load_settings()
    settings.update(data)
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(__import__("json").dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")
    return settings
