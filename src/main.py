from dotenv import load_dotenv
load_dotenv()

from src.config.settings import load_saxo_settings
from src.auth.saxo_oauth import SaxoOAuthClient
from src.brokers.saxo import SaxoBroker


def main():
    print("Starting Saxo OpenAPI test (SIM environment)")

    settings = load_saxo_settings()
    oauth = SaxoOAuthClient(settings)

    print("\n⚠️ OAuth authorization required")
    print("Open this URL in your browser:\n")
    print(
        f"https://connect.saxobank.com/authorize?"
        f"response_type=code&client_id={settings.client_id}"
        f"&redirect_uri={settings.redirect_uri}"
    )

    code = input("\nPaste authorization code here: ").strip()

    try:
        oauth.authenticate(code)
        print("✅ OAuth authentication successful")
    except Exception as e:
        print("❌ OAuth failed:", e)
        return

    broker = SaxoBroker(oauth, settings)

    try:
        accounts = broker.get_accounts()
        balances = broker.get_balance()
    except Exception as e:
        print("❌ API request failed:", e)
        return

    print("\nAccounts:")
    print(accounts)

    print("\nBalances:")
    print(balances)


if __name__ == "__main__":
    main()