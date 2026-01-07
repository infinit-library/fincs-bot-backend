import os
import time
import requests
from typing import Optional
from src.config.setting import SaxoSettings


class SaxoOAuthClient:
    def __init__(self, settings: SaxoSettings):
        self.settings = settings
        self.access_token: Optional[str] = None
        self.expires_at: float = 0.0

        # Load existing token if present
        token = os.getenv("SAXO_ACCESS_TOKEN")
        expires_at = os.getenv("SAXO_ACCESS_TOKEN_EXPIRES_AT")

        if token and expires_at:
            self.access_token = token
            self.expires_at = float(expires_at)

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

        print("✅ OAuth authentication successful")
        print("Access token acquired")

        # (optional) print for debugging
        print(f"Token expires in {expires_in} seconds")

    def get_access_token(self) -> str:
        if not self.access_token:
            raise RuntimeError("OAuth not authenticated yet.")
        if time.time() >= self.expires_at:
            raise RuntimeError("Access token expired.")
        return self.access_token
