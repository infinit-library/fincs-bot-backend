"""
Runtime configuration utilities for Saxo OpenAPI access.

Loads secrets from environment variables (optionally via .env).
"""
from dataclasses import dataclass
import os
from dotenv import load_dotenv


# Load .env for local development; on a VPS you can set env vars directly.
load_dotenv()


@dataclass(frozen=True)
class SaxoSettings:
    client_id: str
    client_secret: str
    redirect_uri: str
    environment: str = "sim"  # "sim" or "live"

    @classmethod
    def from_env(cls) -> "SaxoSettings":
        env = os.getenv("SAXO_ENV", "sim").lower()
        return cls(
            client_id=_require("SAXO_CLIENT_ID"),
            client_secret=_require("SAXO_CLIENT_SECRET"),
            redirect_uri=_require("SAXO_REDIRECT_URI"),
            environment=env,
        )


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value