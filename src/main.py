import logging
import os
from dotenv import load_dotenv

from src.config.setting import load_saxo_settings
from src.auth.saxo_oauth import SaxoOAuthClient
from src.brokers.saxo import SaxoBroker
from src.trading.dry_run import load_limits_from_env, load_signal_from_env, run_dry_run
from src.trading.saxo_pipeline import run_latest_signal_pipeline


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    load_dotenv()
    settings = load_saxo_settings()
    oauth = SaxoOAuthClient(settings)

    print(f"Starting Saxo OpenAPI test ({settings.environment.upper()} environment)")
    print("")
    print("[!] OAuth authorization required")
    print("Open this URL in your browser, log in, and paste the code from the redirect:")
    print(oauth.authorization_url())

    print("")
    code = input("Paste authorization code here: ").strip()

    try:
        oauth.authenticate(code)
        print("[+] OAuth authentication successful")
        if oauth.token:
            print(f"Access token: {oauth.token.access_token}")
            if oauth.token.refresh_token:
                print(f"Refresh token: {oauth.token.refresh_token}")
    except Exception as exc:
        print("[-] OAuth failed:", exc)
        return

    broker = SaxoBroker(oauth)

    try:
        accounts = broker.get_account_info()
        print("Accounts:", accounts)
    except Exception as exc:
        print("Failed to fetch accounts:", exc)

    try:
        balances = broker.get_balance()
        print("Balances:", balances)
    except Exception as exc:
        print("Failed to fetch balances:", exc)

    try:
        pipeline_result = run_latest_signal_pipeline(broker)
        print("Pipeline result:", pipeline_result)
    except Exception as exc:
        print("Pipeline failed:", exc)
        pipeline_result = {"status": "error"}

    if pipeline_result.get("status") == "no_signal":
        signal = load_signal_from_env()
        limits = load_limits_from_env()
        try:
            result = run_dry_run(broker, signal, limits)
            print("Dry-run result:", result)
            if result.order_payload:
                print("Dry-run order payload:", result.order_payload)
            if settings.environment == "live" and not result.live_confirmed:
                print("Live safety gate: set SAXO_LIVE_CONFIRM=I_UNDERSTAND to enable real orders.")
        except Exception as exc:
            print("Dry-run failed:", exc)


if __name__ == "__main__":
    main()
