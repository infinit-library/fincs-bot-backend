import os
from pathlib import Path

import requests
from dotenv import load_dotenv

from src.config.setting import SaxoSettings


ENV_PATH = Path(__file__).resolve().parents[2] / ".env"


def _update_env(values: dict) -> None:
    if ENV_PATH.exists():
        lines = ENV_PATH.read_text(encoding="utf-8").splitlines()
    else:
        lines = []

    keys = set(values.keys())
    updated = []
    seen = set()
    for line in lines:
        if not line or line.strip().startswith("#") or "=" not in line:
            updated.append(line)
            continue
        k, _ = line.split("=", 1)
        k = k.strip()
        if k in values:
            updated.append(f"{k}={values[k]}")
            seen.add(k)
        else:
            updated.append(line)
    for k in keys - seen:
        updated.append(f"{k}={values[k]}")

    ENV_PATH.write_text("\n".join(updated) + "\n", encoding="utf-8")


def main() -> None:
    load_dotenv(override=True)
    settings = SaxoSettings.from_env()

    refresh_token = os.getenv("SAXO_REFRESH_TOKEN")
    if not refresh_token:
        raise SystemExit("Missing SAXO_REFRESH_TOKEN in .env")

    url = f"{settings.auth_base}/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": settings.client_id,
        "client_secret": settings.client_secret,
    }

    response = requests.post(url, data=payload, timeout=30)
    if response.status_code not in (200, 201):
        raise SystemExit(f"Refresh failed ({response.status_code}): {response.text}")

    data = response.json()
    access_token = data.get("access_token")
    expires_in = data.get("expires_in")
    new_refresh = data.get("refresh_token")
    refresh_expires = data.get("refresh_token_expires_in")

    if not access_token or not expires_in:
        raise SystemExit("Missing access_token or expires_in in response")

    updates = {
        "SAXO_ACCESS_TOKEN": access_token,
        "SAXO_ACCESS_TOKEN_TTL": str(expires_in),
    }
    if new_refresh:
        updates["SAXO_REFRESH_TOKEN"] = new_refresh
    if refresh_expires:
        updates["SAXO_REFRESH_TOKEN_TTL"] = str(refresh_expires)

    _update_env(updates)
    print("Token refreshed and .env updated")


if __name__ == "__main__":
    main()
