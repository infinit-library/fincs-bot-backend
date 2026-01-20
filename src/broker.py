import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

from src import runtime_config
from .config.setting import SaxoSettings
from .auth.saxo_oauth import SaxoOAuthClient, Token


@dataclass
class BrokerResult:
    ok: bool
    order_id: Optional[str]
    error: Optional[str]
    payload: Optional[Dict[str, Any]]


ENV_PATH = Path(__file__).resolve().parent.parent / ".env"



def _load_env_file() -> None:
    if not ENV_PATH.exists():
        return
    for raw in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ[key] = val


def _update_env_vars(updates: Dict[str, str]) -> None:
    existing_lines = []
    if ENV_PATH.exists():
        existing_lines = ENV_PATH.read_text(encoding="utf-8").splitlines()

    keys = set(updates.keys())
    new_lines = []
    seen = set()

    for line in existing_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            new_lines.append(line)
            continue
        key, _ = stripped.split("=", 1)
        if key in updates:
            new_lines.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            new_lines.append(line)

    for key in keys - seen:
        new_lines.append(f"{key}={updates[key]}")

    ENV_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

    for key, val in updates.items():
        os.environ[key] = str(val)


def _persist_saxo_tokens(token: Token) -> None:
    updates = {
        "SAXO_ACCESS_TOKEN": token.access_token,
    }
    if token.refresh_token:
        updates["SAXO_REFRESH_TOKEN"] = token.refresh_token
    updates["SAXO_TOKEN_EXPIRES_AT"] = str(int(token.expires_at))
    if getattr(token, "refresh_expires_at", None):
        updates["SAXO_REFRESH_TOKEN_EXPIRES_AT"] = str(int(token.refresh_expires_at))
    _update_env_vars(updates)


def _load_oauth_from_env(oauth: SaxoOAuthClient) -> SaxoOAuthClient:
    _load_env_file()
    access = os.getenv("SAXO_ACCESS_TOKEN") or ""
    refresh = os.getenv("SAXO_REFRESH_TOKEN")
    expires_raw = os.getenv("SAXO_TOKEN_EXPIRES_AT")
    refresh_expires_raw = os.getenv("SAXO_REFRESH_TOKEN_EXPIRES_AT")
    expires_at = float(expires_raw) if expires_raw else 0.0
    refresh_expires_at = float(refresh_expires_raw) if refresh_expires_raw else 0.0

    if access or refresh:
        oauth.token = Token(access_token=access, refresh_token=refresh, expires_at=expires_at, refresh_expires_at=refresh_expires_at)
        if refresh and oauth.token.is_expired:
            oauth.refresh()
            if oauth.token:
                _persist_saxo_tokens(oauth.token)

    return oauth


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
        oauth = _load_oauth_from_env(oauth)
        account_key = os.getenv("SAXO_ACCOUNT_KEY")
        client_key = os.getenv("SAXO_CLIENT_KEY")
        uic_map = _load_uic_map(runtime_config.load_settings())
        from .brokers.saxo import SaxoBroker

        return SaxoBroker(oauth, settings, account_key=account_key, client_key=client_key, uic_map=uic_map)
    raise ValueError(f"Unsupported broker: {name}")
