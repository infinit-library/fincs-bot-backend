"""
Runtime configuration utilities for Saxo OpenAPI access.
"""

from dataclasses import dataclass
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()


@dataclass(frozen=True)
class SaxoSettings:
    client_id: str
    client_secret: str
    redirect_uri: str
    environment: str
    base_url: str
    auth_base: str

    @classmethod
    def from_env(cls) -> "SaxoSettings":
        env = os.getenv("SAXO_ENV", "sim").lower()

        return cls(
            client_id=_require("SAXO_CLIENT_ID"),
            client_secret=_require("SAXO_CLIENT_SECRET"),
            redirect_uri=_require("SAXO_REDIRECT_URI"),
            environment=env,
            base_url=_base_url(env),
            auth_base=_auth_base(env),
        )


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    value = value.strip()
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        value = value[1:-1].strip()
    return value


def _base_url(env: str) -> str:
    if env == "live":
        return "https://gateway.saxobank.com/openapi"
    return "https://gateway.saxobank.com/sim/openapi"


def _auth_base(env: str) -> str:
    if env == "live":
        return "https://live.logonvalidation.net"
    return "https://sim.logonvalidation.net"


def load_saxo_settings() -> SaxoSettings:
    return SaxoSettings.from_env()
