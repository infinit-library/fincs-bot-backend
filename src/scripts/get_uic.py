import argparse
import os
import json
from typing import Any, Dict

import requests
from dotenv import load_dotenv


def _require(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise SystemExit(f"Missing env var: {name}")
    return val


def _env_base() -> str:
    env = os.getenv("SAXO_ENV", "sim").lower()
    if env not in {"sim", "live"}:
        raise SystemExit("SAXO_ENV must be 'sim' or 'live'")
    return "https://gateway.saxobank.com/openapi/sim/openapi" if env == "sim" else "https://gateway.saxobank.com/openapi"


def fetch_instruments(token: str, keyword: str) -> Dict[str, Any]:
    base = _env_base()
    url = f"{base}/ref/v1/instruments"
    params = {"AssetTypes": "FxSpot", "Keywords": keyword}
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params=params, timeout=10)
    if resp.status_code != 200:
        raise SystemExit(f"HTTP {resp.status_code}: {resp.text}")
    return resp.json()


def find_uic(data: Dict[str, Any], symbol: str) -> int:
    for item in data.get("Data", []):
        if (item.get("Symbol") or "").upper() != symbol.upper():
            continue
        uic = item.get("Uic")
        if uic is None:
            # Some ref responses use Identifier instead of Uic
            uic = item.get("Identifier")
        if uic is None:
            continue
        return int(uic)
    raise SystemExit(f"UIC not found for symbol {symbol}")


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Fetch Saxo UIC for an FX symbol")
    parser.add_argument("symbol", nargs="?", default="USDJPY")
    args = parser.parse_args()

    token = _require("SAXO_ACCESS_TOKEN")
    data = fetch_instruments(token, args.symbol)
    try:
        uic = find_uic(data, args.symbol)
    except SystemExit:
        print("No exact match found; raw response:")
        print(json.dumps(data, indent=2)[:4000])
        raise
    print(f"{args.symbol} UIC: {uic}")


if __name__ == "__main__":
    main()
