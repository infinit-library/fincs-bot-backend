import os
import json
import requests
from dotenv import load_dotenv
from src.config.setting import SaxoSettings


def main() -> None:
    load_dotenv(override=True)
    settings = SaxoSettings.from_env()

    token = os.getenv("SAXO_ACCESS_TOKEN")
    account_key = os.getenv("SAXO_ACCOUNT_KEY")
    client_key = os.getenv("SAXO_CLIENT_KEY")
    if not token:
        raise SystemExit("Missing SAXO_ACCESS_TOKEN")
    if not account_key:
        raise SystemExit("Missing SAXO_ACCOUNT_KEY")

    url = f"{settings.base_url}/trade/v2/orders/precheck"
    payload = {
        "AccountKey": account_key,
        "Uic": 22,
        "AssetType": "FxSpot",
        "Amount": 10000,
        "BuySell": "Buy",
        "OrderType": "Market",
        "ManualOrder": True,
    }
    if client_key:
        payload["ClientKey"] = client_key

    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=payload,
        timeout=30,
    )
    print(f"status: {resp.status_code}")
    print(resp.text)


if __name__ == "__main__":
    main()
