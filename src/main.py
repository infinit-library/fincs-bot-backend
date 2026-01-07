from src.config.setting import SaxoSettings
from src.auth.saxo_oauth import SaxoOAuthClient
import urllib.parse


def main():
    settings = SaxoSettings.from_env()

    print(f"Starting Saxo OpenAPI test ({settings.environment.upper()} environment)\n")

    oauth = SaxoOAuthClient(settings)

    # Step 1: show authorization URL
    auth_url = (
        f"{settings.auth_base}/authorize?"
        f"response_type=code"
        f"&client_id={settings.client_id}"
        f"&redirect_uri={settings.redirect_uri}"
    )

    print("⚠️ OAuth authorization required")
    print("Open this URL in your browser:\n")
    print(auth_url)
    print()

    # Step 2: user pastes ?code=XXXX value
    code_input = input("Paste authorization code here: ").strip()
    code = code_input
    if "code=" in code_input:
        parsed = urllib.parse.urlparse(code_input)
        qs = urllib.parse.parse_qs(parsed.query)
        code = (qs.get("code") or [""])[0]
    if "#" in code:
        code = code.split("#", 1)[0].strip()

    oauth.authenticate(code)

    print("\n🎉 Saxo OAuth setup completed successfully!")


if __name__ == "__main__":
    main()
