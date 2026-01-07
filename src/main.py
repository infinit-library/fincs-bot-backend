from src.config.setting import SaxoSettings
from src.auth.saxo_oauth import SaxoOAuthClient


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
    code = input("Paste authorization code here: ").strip()

    oauth.authenticate(code)

    print("\n🎉 Saxo OAuth setup completed successfully!")


if __name__ == "__main__":
    main()
