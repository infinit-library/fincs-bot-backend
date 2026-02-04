import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests

from src.config.setting import SaxoSettings

logger = logging.getLogger(__name__)


@dataclass
class Token:
    access_token: str
    refresh_token: Optional[str]
    expires_at: float  # epoch seconds
    refresh_expires_at: Optional[float] = None

    @property
    def is_expired(self) -> bool:
        return time.time() >= self.expires_at - 30  # small buffer


class SaxoOAuthClient:
    """Handles OAuth2 for Saxo OpenAPI (SIM/LIVE)."""

    def __init__(self, settings: SaxoSettings) -> None:
        self.settings = settings
        self.token: Optional[Token] = None
        self.auth_base = "https://sim.logonvalidation.net" if settings.environment == "sim" else "https://live.logonvalidation.net"
        self.api_base = settings.base_url

    def authorization_url(self, state: str = "fxbot") -> str:
        return (
            f"{self.auth_base}/authorize?response_type=code"
            f"&client_id={self.settings.client_id}"
            f"&redirect_uri={self.settings.redirect_uri}"
            f"&state={state}"
        )

    def authenticate(self, code: str) -> None:
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.settings.redirect_uri,
            "client_id": self.settings.client_id,
            "client_secret": self.settings.client_secret,
        }
        self._exchange_token(data)

    def refresh(self) -> None:
        if not self.token or not self.token.refresh_token:
            raise RuntimeError("No refresh_token available; re-run interactive auth")
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.token.refresh_token,
            "client_id": self.settings.client_id,
            "client_secret": self.settings.client_secret,
        }
        self._exchange_token(data)

    def _exchange_token(self, data: Dict[str, str]) -> None:
        url = f"{self.auth_base}/token"
        resp = requests.post(url, data=data, timeout=15)
        logger.debug("POST %s -> %s", url, resp.status_code)
        if resp.status_code < 200 or resp.status_code >= 300:
            raise RuntimeError(f"Token request failed: {resp.status_code}")
        payload = resp.json()
        access = payload.get("access_token")
        refresh = payload.get("refresh_token")
        expires_in = payload.get("expires_in", 0)
        refresh_expires_in = payload.get("refresh_token_expires_in")
        refresh_expires_at = None
        if refresh_expires_in is not None:
            try:
                refresh_expires_at = time.time() + int(refresh_expires_in)
            except Exception:
                refresh_expires_at = None
        if not access:
            raise RuntimeError("Token response missing access_token")
        self.token = Token(
            access_token=access,
            refresh_token=refresh,
            expires_at=time.time() + int(expires_in or 0),
            refresh_expires_at=refresh_expires_at,
        )

    def get_access_token(self) -> str:
        if not self.token:
            raise RuntimeError("Not authenticated. Call authenticate() first.")
        disable_refresh = os.getenv("SAXO_DISABLE_REFRESH", "").strip().lower() in {"1", "true", "yes"}
        if self.token.is_expired and not disable_refresh:
            try:
                self.refresh()
            except Exception as exc:
                raise RuntimeError(
                    "Token refresh failed. Ensure SAXO_ENV and client credentials match the token environment, "
                    "or re-authorize to obtain new tokens. "
                    f"Original error: {exc}"
                )
        return self.token.access_token

    def api_post(self, path: str, json: Optional[Dict[str, Any]] = None) -> requests.Response:
        token = self.get_access_token()
        url = f"{self.api_base}{path}"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.post(url, headers=headers, json=json, timeout=15)
        logger.debug("POST %s -> %s", url, resp.status_code)
        return resp

    def api_get(self, path: str, params: Optional[Dict[str, str]] = None) -> requests.Response:
        token = self.get_access_token()
        url = f"{self.api_base}{path}"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        logger.debug("GET %s -> %s %s", url, resp.status_code, resp.text[:500])
        return resp
