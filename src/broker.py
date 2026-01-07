import json
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests


@dataclass
class BrokerResult:
    ok: bool
    order_id: Optional[str]
    error: Optional[str]
    payload: Optional[Dict[str, Any]]


def _oanda_base_url(env: str) -> str:
    env = (env or "practice").lower()
    if env == "live":
        return "https://api-fxtrade.oanda.com/v3"
    return "https://api-fxpractice.oanda.com/v3"


class OandaBroker:
    name = "oanda"

    def __init__(self) -> None:
        self.api_key = os.getenv("OANDA_API_KEY")
        self.account_id = os.getenv("OANDA_ACCOUNT_ID")
        self.env = os.getenv("OANDA_ENV", "practice")
        self.base_url = _oanda_base_url(self.env)
        self._positions_cache: Optional[dict] = None

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def place_market_order(
        self,
        instrument: str,
        side: str,
        units: int,
        sl_price: Optional[float] = None,
        tp_price: Optional[float] = None,
        client_id: Optional[str] = None,
        dry_run: bool = True,
    ) -> BrokerResult:
        if not dry_run and (not self.api_key or not self.account_id):
            return BrokerResult(False, None, "Missing OANDA_API_KEY or OANDA_ACCOUNT_ID", None)

        payload: Dict[str, Any] = {
            "order": {
                "type": "MARKET",
                "instrument": instrument,
                "units": str(units),
                "timeInForce": "FOK",
            }
        }

        if client_id:
            payload["order"]["clientExtensions"] = {"id": client_id}

        if sl_price is not None:
            payload["order"]["stopLossOnFill"] = {"price": f"{sl_price:.5f}"}
        if tp_price is not None:
            payload["order"]["takeProfitOnFill"] = {"price": f"{tp_price:.5f}"}

        if dry_run:
            return BrokerResult(True, f"dryrun-{client_id or instrument}", None, payload)

        url = f"{self.base_url}/accounts/{self.account_id}/orders"
        try:
            resp = requests.post(url, headers=self._headers(), data=json.dumps(payload), timeout=10)
            if resp.status_code >= 400:
                return BrokerResult(False, None, f"{resp.status_code}: {resp.text}", payload)
            data = resp.json()
            order_id = data.get("orderCreateTransaction", {}).get("id") or data.get("lastTransactionID")
            return BrokerResult(True, str(order_id) if order_id else None, None, payload)
        except Exception as exc:  # pragma: no cover - network errors
            return BrokerResult(False, None, str(exc), payload)

    def get_price(self, instrument: str) -> BrokerResult:
        if not self.api_key or not self.account_id:
            return BrokerResult(False, None, "Missing OANDA_API_KEY or OANDA_ACCOUNT_ID", None)
        url = f"{self.base_url}/accounts/{self.account_id}/pricing?instruments={instrument}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=10)
            if resp.status_code >= 400:
                return BrokerResult(False, None, f"{resp.status_code}: {resp.text}", None)
            data = resp.json()
            prices = data.get("prices", [])
            if not prices:
                return BrokerResult(False, None, "No pricing data", None)
            bids = prices[0].get("bids", [])
            asks = prices[0].get("asks", [])
            if not bids or not asks:
                return BrokerResult(False, None, "No bid/ask", None)
            bid = float(bids[0]["price"])
            ask = float(asks[0]["price"])
            mid = (bid + ask) / 2
            spread = ask - bid
            return BrokerResult(True, None, None, {"mid": mid, "spread": spread})
        except Exception as exc:
            return BrokerResult(False, None, str(exc), None)

    def get_open_position_units(self, instrument: str) -> int:
        if not self.api_key or not self.account_id:
            return 0
        # Try cached positions first
        if self._positions_cache and instrument in self._positions_cache:
            return self._positions_cache[instrument]
        url = f"{self.base_url}/accounts/{self.account_id}/positions/{instrument}"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=10)
            if resp.status_code == 404:
                return 0
            if resp.status_code >= 400:
                return 0
            data = resp.json().get("position", {})
            long_units = int(float(data.get("long", {}).get("units", "0") or 0))
            short_units = int(float(data.get("short", {}).get("units", "0") or 0))
            units = long_units + short_units
            if self._positions_cache is None:
                self._positions_cache = {}
            self._positions_cache[instrument] = units
            return units
        except Exception:
            return 0

    def refresh_positions(self) -> dict:
        positions: dict = {}
        if not self.api_key or not self.account_id:
            return positions
        url = f"{self.base_url}/accounts/{self.account_id}/positions"
        try:
            resp = requests.get(url, headers=self._headers(), timeout=10)
            if resp.status_code >= 400:
                return positions
            data = resp.json().get("positions", [])
            for p in data:
                instrument = p.get("instrument")
                long_units = int(float(p.get("long", {}).get("units", "0") or 0))
                short_units = int(float(p.get("short", {}).get("units", "0") or 0))
                positions[instrument] = long_units + short_units
            self._positions_cache = positions
            return positions
        except Exception:
            return positions


def to_oanda_instrument(pair: str) -> str:
    # Convert USDJPY -> USD_JPY
    pair = pair.upper()
    if "_" in pair:
        return pair
    if len(pair) == 6:
        return pair[:3] + "_" + pair[3:]
    return pair


def get_broker(name: str):
    name = (name or "").lower()
    if name in ("oanda", "", None):
        return OandaBroker()
    raise ValueError(f"Unsupported broker: {name}")
