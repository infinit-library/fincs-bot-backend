import json
from typing import Dict, Optional

import requests

from src.auth.saxo_oauth import SaxoOAuthClient
from src.config.setting import SaxoSettings
from src.broker import BrokerResult


class SaxoBroker:
    name = "saxo"

    def __init__(self, oauth: SaxoOAuthClient, settings: SaxoSettings, account_key: Optional[str] = None, client_key: Optional[str] = None, uic_map: Optional[Dict[str, int]] = None):
        self.oauth = oauth
        self.base_url = settings.base_url
        self.account_key = account_key
        self.client_key = client_key
        self.uic_map = {k.upper(): int(v) for k, v in (uic_map or {}).items()}
        self._positions_cache: Dict[int, int] = {}

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.oauth.get_access_token()}",
            "Content-Type": "application/json",
        }

    def _resolve_uic(self, instrument) -> Optional[int]:
        if instrument is None:
            return None
        if isinstance(instrument, int):
            return instrument
        key = str(instrument).upper()
        return self.uic_map.get(key)

    def _resolve_pair(self, uic: int) -> Optional[str]:
        for pair, mapped in self.uic_map.items():
            if mapped == uic:
                return pair
        return None

    def get_accounts(self) -> dict:
        url = f"{self.base_url}/port/v1/accounts/me"
        response = requests.get(url, headers=self._headers(), timeout=10)
        response.raise_for_status()
        return response.json()

    def get_balance(self) -> dict:
        url = f"{self.base_url}/port/v1/balances"
        params = {}
        if self.account_key:
            params["AccountKey"] = self.account_key
        if self.client_key:
            params["ClientKey"] = self.client_key
        response = requests.get(url, headers=self._headers(), params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def get_equity(self) -> Optional[float]:
        try:
            data = self.get_balance()
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        for key in ("TotalEquity", "NetEquityForMargin", "Equity", "AccountValue"):
            val = data.get(key)
            if val is not None:
                try:
                    return float(val)
                except Exception:
                    return None
        return None


    def precheck_order(self, instrument, side: str, units: int) -> Optional[float]:
        uic = self._resolve_uic(instrument)
        if uic is None or not self.account_key:
            return None
        buy_sell = "Buy" if str(side).upper() in ("LONG", "BUY") else "Sell"
        amount = abs(int(units))
        payload = {
            "AccountKey": self.account_key,
            "Uic": uic,
            "AssetType": "FxSpot",
            "Amount": amount,
            "BuySell": buy_sell,
            "OrderType": "Market",
        }
        url = f"{self.base_url}/trade/v2/orders/precheck"
        try:
            response = requests.post(url, headers=self._headers(), data=json.dumps(payload), timeout=10)
            if response.status_code >= 400:
                return None
            data = response.json()
            for key in ("MarginRequirement", "MarginRequired"):
                val = data.get(key)
                if val is not None:
                    return float(val)
        except Exception:
            return None
        return None


    def get_price(self, instrument) -> BrokerResult:
        uic = self._resolve_uic(instrument)
        if uic is None:
            return BrokerResult(False, None, "Missing UIC for instrument", None)
        url = f"{self.base_url}/trade/v1/prices"
        params = {
            "Uic": uic,
            "AssetType": "FxSpot",
        }
        try:
            response = requests.get(url, headers=self._headers(), params=params, timeout=10)
            if response.status_code >= 400:
                return BrokerResult(False, None, f"{response.status_code}: {response.text}", None)
            data = response.json()
            quote = None
            if isinstance(data, dict):
                if isinstance(data.get("Quotes"), list) and data["Quotes"]:
                    quote = data["Quotes"][0]
                elif isinstance(data.get("Quote"), dict):
                    quote = data["Quote"]
                elif isinstance(data.get("Prices"), list) and data["Prices"]:
                    quote = data["Prices"][0]
            if not quote:
                return BrokerResult(False, None, "No quote data", data)
            bid = quote.get("Bid")
            ask = quote.get("Ask")
            if bid is None or ask is None:
                return BrokerResult(False, None, "No bid/ask", data)
            bid_f = float(bid)
            ask_f = float(ask)
            mid = (bid_f + ask_f) / 2
            spread = ask_f - bid_f
            return BrokerResult(True, None, None, {"mid": mid, "spread": spread})
        except Exception as exc:  # pragma: no cover
            return BrokerResult(False, None, str(exc), None)

    def refresh_positions(self) -> dict:
        if not self.account_key:
            return {}
        url = f"{self.base_url}/port/v1/positions/me"
        params = {
            "AccountKey": self.account_key,
            "$top": 200,
        }
        try:
            response = requests.get(url, headers=self._headers(), params=params, timeout=10)
            if response.status_code >= 400:
                return {}
            data = response.json()
            positions: Dict[int, int] = {}
            for item in data.get("Data", []) if isinstance(data, dict) else []:
                uic = item.get("Uic")
                amount = None
                base = item.get("PositionBase") if isinstance(item, dict) else None
                if isinstance(base, dict):
                    amount = base.get("Amount")
                if uic is None or amount is None:
                    continue
                positions[int(uic)] = int(float(amount))
            self._positions_cache = positions
            return positions
        except Exception:
            return {}

    def get_open_position_units(self, instrument) -> int:
        if instrument in self._positions_cache:
            return self._positions_cache[instrument]
        positions = self.refresh_positions()
        if instrument in positions:
            return positions[instrument]
        return 0

    def place_market_order(
        self,
        instrument,
        side: str,
        units: int,
        sl_price: Optional[float] = None,
        tp_price: Optional[float] = None,
        client_id: Optional[str] = None,
        dry_run: bool = True,
    ) -> BrokerResult:
        uic = self._resolve_uic(instrument)
        if uic is None:
            return BrokerResult(False, None, "Missing UIC for instrument", None)
        if not self.account_key and not dry_run:
            return BrokerResult(False, None, "Missing SAXO_ACCOUNT_KEY", None)

        buy_sell = "Buy" if str(side).upper() in ("LONG", "BUY") else "Sell"
        amount = abs(int(units))

        payload = {
            "AccountKey": self.account_key,
            "Uic": uic,
            "AssetType": "FxSpot",
            "Amount": amount,
            "BuySell": buy_sell,
            "OrderType": "Market",
        }
        if client_id:
            payload["ExternalReference"] = str(client_id)

        if dry_run:
            return BrokerResult(True, f"dryrun-{client_id or uic}", None, payload)

        url = f"{self.base_url}/trade/v2/orders"
        try:
            response = requests.post(url, headers=self._headers(), data=json.dumps(payload), timeout=10)
            if response.status_code >= 400:
                return BrokerResult(False, None, f"{response.status_code}: {response.text}", payload)
            data = response.json()
            order_id = data.get("OrderId") or data.get("orderId") or data.get("Id")
            return BrokerResult(True, str(order_id) if order_id else None, None, payload)
        except Exception as exc:  # pragma: no cover
            return BrokerResult(False, None, str(exc), payload)
