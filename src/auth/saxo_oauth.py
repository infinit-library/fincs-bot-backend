import os
import time
from pathlib import Path
from typing import Optional

import requests

from src.config.setting import SaxoSettings


ENV_PATH = Path(__file__).resolve().parents[2] / ".env"


class SaxoOAuthClient:
    def __init__(self, settings: SaxoSettings):
        self.settings = settings
        self.access_token: Optional[str] = None
        self.expires_at: float = 0.0

        # Load existing token if present
        token = os.getenv("SAXO_ACCESS_TOKEN")
        expires_at = os.getenv("SAXO_ACCESS_TOKEN_EXPIRES_AT")
        ttl = os.getenv("SAXO_ACCESS_TOKEN_TTL")

        if token and expires_at:
            self.access_token = token
            self.expires_at = float(expires_at)
        elif token and ttl:
            self.access_token = token
            try:
                self.expires_at = time.time() + float(ttl) - 30
            except ValueError:
                self.expires_at = 0.0
        elif token:
            # Treat missing TTL as expired to fail closed
            self.access_token = None
            self.expires_at = 0.0

    def get_authorize_url(self) -> str:
        return (
            f"{self.settings.auth_base}/authorize"
            f"?response_type=code"
            f"&client_id={self.settings.client_id}"
            f"&redirect_uri={self.settings.redirect_uri}"
        )

    def authenticate(self, authorization_code: str) -> None:
        url = f"{self.settings.auth_base}/token"

        payload = {
            "grant_type": "authorization_code",
            "code": authorization_code,
            "redirect_uri": self.settings.redirect_uri,
            "client_id": self.settings.client_id,
            "client_secret": self.settings.client_secret,
        }

        for attempt in range(3):
            try:
                response = requests.post(url, data=payload, timeout=60)
                break
            except requests.exceptions.ReadTimeout:
                if attempt >= 2:
                    raise
                continue
        if response.status_code not in (200, 201):
            raise RuntimeError(f"OAuth failed ({response.status_code}): {response.text}")

        data = response.json()
        self._store_token(data)

    def _store_token(self, data: dict) -> None:
        self.access_token = data["access_token"]
        expires_in = int(data["expires_in"])
        self.expires_at = time.time() + expires_in - 30

        print("OAuth authentication successful")
        print("Access token acquired")

        # (optional) print for debugging
        print(f"Access token: {self.access_token}")
        if "refresh_token" in data:
            print(f"Refresh token: {data['refresh_token']}")
        if "refresh_token_expires_in" in data:
            print(f"Refresh token expires in {data['refresh_token_expires_in']} seconds")
        print(f"Token expires in {expires_in} seconds")

    def _update_env(self, values: dict) -> None:
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
            key, _ = line.split("=", 1)
            key = key.strip()
            if key in values:
                updated.append(f"{key}={values[key]}")
                seen.add(key)
            else:
                updated.append(line)
        for key in keys - seen:
            updated.append(f"{key}={values[key]}")

        ENV_PATH.write_text("\n".join(updated) + "\n", encoding="utf-8")

    def _get_refresh_token(self) -> Optional[str]:
        return os.getenv("SAXO_REFRESH_TOKEN") or os.getenv("SAXO_ACCESS_REFRESH_TOKEN")

    def _auto_refresh_enabled(self) -> bool:
        return os.getenv("SAXO_AUTO_REFRESH", "").lower() == "true"

    def _refresh_threshold(self) -> int:
        try:
            return int(os.getenv("SAXO_AUTO_REFRESH_THRESHOLD", "60"))
        except ValueError:
            return 60

    def refresh_access_token(self) -> None:
        refresh_token = self._get_refresh_token()
        if not refresh_token:
            raise RuntimeError("Missing SAXO_REFRESH_TOKEN for auto-refresh.")

        url = f"{self.settings.auth_base}/token"
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.settings.client_id,
            "client_secret": self.settings.client_secret,
        }

        response = requests.post(url, data=payload, timeout=30)
        if response.status_code not in (200, 201):
            raise RuntimeError(f"Refresh failed ({response.status_code}): {response.text}")

        data = response.json()
        access_token = data.get("access_token")
        expires_in = data.get("expires_in")
        new_refresh = data.get("refresh_token")
        refresh_expires = data.get("refresh_token_expires_in")

        if not access_token or not expires_in:
            raise RuntimeError("Missing access_token or expires_in in refresh response")

        self.access_token = access_token
        self.expires_at = time.time() + int(expires_in) - 30

        updates = {
            "SAXO_ACCESS_TOKEN": access_token,
            "SAXO_ACCESS_TOKEN_TTL": str(expires_in),
            "SAXO_ACCESS_TOKEN_EXPIRES_AT": str(int(self.expires_at)),
        }
        if new_refresh:
            updates["SAXO_REFRESH_TOKEN"] = new_refresh
        if refresh_expires:
            updates["SAXO_REFRESH_TOKEN_TTL"] = str(refresh_expires)

        self._update_env(updates)

    def get_access_token(self) -> str:
        if not self.access_token:
            raise RuntimeError("OAuth not authenticated yet.")

        threshold = self._refresh_threshold()
        if time.time() >= (self.expires_at - threshold):
            if self._auto_refresh_enabled():
                self.refresh_access_token()
            else:
                raise RuntimeError("Access token expired.")

        return self.access_token