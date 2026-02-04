import os
from pathlib import Path

from src.auth.saxo_oauth import SaxoOAuthClient
from src.config.setting import SaxoSettings

ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"


def _update_env_vars(updates: dict) -> None:
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


def main() -> None:
    settings = SaxoSettings.from_env()
    oauth = SaxoOAuthClient(settings)

    print(f"SAXO_ENV={settings.environment.upper()}")
    print("Open this URL in your browser and login:")
    print(oauth.authorization_url())
    print("")

    code = input("Paste authorization code here: ").strip()
    if not code:
        raise SystemExit("Missing code")

    oauth.authenticate(code)
    if not oauth.token:
        raise SystemExit("Token exchange failed")

    updates = {
        "SAXO_ACCESS_TOKEN": oauth.token.access_token,
        "SAXO_TOKEN_EXPIRES_AT": str(int(oauth.token.expires_at)),
    }
    if oauth.token.refresh_token:
        updates["SAXO_REFRESH_TOKEN"] = oauth.token.refresh_token
    _update_env_vars(updates)

    print("\n[OK] Tokens stored in .env")
    print("SAXO_ACCESS_TOKEN set")
    if oauth.token.refresh_token:
        print("SAXO_REFRESH_TOKEN set")


if __name__ == "__main__":
    main()
