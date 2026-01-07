import requests
import time
from typing import Optional
from config.settings import SaxoSettings


class SaxoOAuthClient:
    def __init__(self, settings: SaxoSettings):
        self.settings = settings
        self.access_token: Optional[str] = None
        self.expires_at: float = 0.0

    def authenticate(self, authorization_code: str) -> None:
        """
        One-time OAuth step.
        Client gets authorization_code manually from browser.
        """
        url = "https://connect.saxobank.com/authorize/token"
        payload = {
            "grant_type": "authorization_code",
            "code": authorization_code,
            "redirect_uri": self.settings.redirect_uri,
            "client_id": self.settings.client_id,
            "client_secret": self.settings.client_secret,
        }

        response = requests.post(url, data=payload, timeout=15)
        response.raise_for_status()

        data = response.json()
        self._store_token(data)

    def _store_token(self, data: dict) -> None:
        self.access_token = data["access_token"]
        expires_in = int(data["expires_in"])
        self.expires_at = time.time() + expires_in - 30

    def get_access_token(self) -> str:
        if not self.access_token:
            raise RuntimeError("OAuth not authenticated yet.")
        if time.time() >= self.expires_at:
            raise RuntimeError("Access token expired. Refresh flow required.")
        return self.access_token
