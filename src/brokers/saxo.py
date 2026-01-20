import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

from src.auth.saxo_oauth import SaxoOAuthClient
from src.brokers.base import BaseBroker

logger = logging.getLogger(__name__)

# Default UIC for USDJPY must be provided; set via env or caller param.
DEFAULT_USDJPY_UIC_ENV = "SAXO_USDJPY_UIC"


@dataclass
class SaxoResult:
    ok: bool
    order_id: Optional[str]
    error: Optional[str]
    payload: Optional[Dict[str, Any]]


class SaxoBroker(BaseBroker):
    name = "saxo"

    def __init__(
        self,
        oauth: SaxoOAuthClient,
        settings: Optional[Any] = None,
        account_key: Optional[str] = None,
        client_key: Optional[str] = None,
        uic_map: Optional[Dict[str, int]] = None,
        usd_jpy_uic: Optional[str] = None,
    ) -> None:
        self.oauth = oauth
        self.settings = settings
        self.account_key = (account_key or os.getenv("SAXO_ACCOUNT_KEY", "")).strip() or None
        self.client_key = (client_key or os.getenv("SAXO_CLIENT_KEY", "")).strip() or None
        self.uic_map = {str(k).upper(): int(v) for k, v in (uic_map or {}).items() if k is not None}
        if usd_jpy_uic is None:
            if self.uic_map.get("USDJPY"):
                usd_jpy_uic = str(self.uic_map["USDJPY"])
            else:
                usd_jpy_uic = os.getenv(DEFAULT_USDJPY_UIC_ENV)
        self.usd_jpy_uic = usd_jpy_uic

    def _handle_response(self, resp) -> Dict[str, Any]:
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 401:
            raise PermissionError("401 Unauthorized: check token scope.")
        if resp.status_code == 403:
            raise PermissionError("403 Forbidden: insufficient privileges or wrong environment.")
        if resp.status_code == 404:
            raise FileNotFoundError("404 Not Found: verify endpoint or account.")
        raise RuntimeError(f"Unexpected status {resp.status_code}: {resp.text}")

    def _balance_params(self) -> Dict[str, str]:
        if self.client_key:
            return {"ClientKey": self.client_key}
        if self.account_key:
            return {"AccountKey": self.account_key}
        return {}

    def _position_params(self) -> Dict[str, str]:
        if self.account_key:
            return {"AccountKey": self.account_key}
        if self.client_key:
            return {"ClientKey": self.client_key}
        return {}

    def _order_params(self) -> Dict[str, str]:
        if self.account_key:
            return {"AccountKey": self.account_key}
        if self.client_key:
            return {"ClientKey": self.client_key}
        return {}

    def get_account_info(self) -> Dict[str, Any]:
        resp = self.oauth.api_get("/port/v1/accounts/me")
        return self._handle_response(resp)

    def get_balance(self) -> Dict[str, Any]:
        params = self._balance_params()
        resp = self.oauth.api_get("/port/v1/balances", params=params or None)
        return self._handle_response(resp)

    def get_equity(self) -> Optional[float]:
        data = self.get_balance()
        return _extract_equity(data)

    def get_positions(self) -> Dict[str, Any]:
        params = self._position_params()
        resp = self.oauth.api_get("/port/v1/positions", params=params or None)
        return self._handle_response(resp)

    def refresh_positions(self) -> Dict[int, int]:
        payload = self.get_positions()
        return _extract_positions(payload)

    def get_open_position_units(self, uic: int) -> int:
        positions = self.refresh_positions()
        return int(positions.get(int(uic), 0))

    def get_price(self, symbol: str) -> Dict[str, Any]:
        symbol = symbol.upper()
        uic = None
        if symbol in self.uic_map:
            uic = self.uic_map[symbol]
        elif symbol == "USDJPY":
            uic = self.usd_jpy_uic or _load_default_uic()
        if not uic:
            raise ValueError("Missing UIC for symbol")

        params = {"Uics": uic, "AssetType": "FxSpot"}
        resp = self.oauth.api_get("/trade/v1/prices", params=params)
        return self._handle_response(resp)

    def precheck_order(self, uic: int, direction: str, units: int) -> Optional[float]:
        payload: Dict[str, Any] = {
            "Uic": int(uic),
            "AssetType": "FxSpot",
            "Amount": int(abs(units)),
            "BuySell": "Buy" if direction.upper() == "BUY" else "Sell",
            "OrderType": "Market",
            "ManualOrder": True,
        }
        payload.update(self._order_params())
        try:
            resp = self.oauth.api_post("/trade/v2/orders/precheck", json=payload)
            data = self._handle_response(resp)
        except Exception:
            return None
        return _extract_margin_required(data)

    def place_market_order(
        self,
        instrument: Optional[int] = None,
        side: Optional[str] = None,
        units: Optional[int] = None,
        sl_price: Optional[float] = None,
        tp_price: Optional[float] = None,
        client_id: Optional[str] = None,
        dry_run: bool = False,
        payload: Optional[Dict[str, Any]] = None,
    ) -> SaxoResult:
        if payload is None:
            if instrument is None or side is None or units is None:
                return SaxoResult(False, None, "Missing order parameters", None)
            payload = {
                "Uic": int(instrument),
                "AssetType": "FxSpot",
                "Amount": int(abs(units)),
                "BuySell": "Buy" if side.upper() == "BUY" else "Sell",
                "OrderType": "Market",
                "ManualOrder": True,
            }
            if client_id:
                payload["ExternalReference"] = client_id
            payload.update(self._order_params())

        if dry_run:
            return SaxoResult(True, None, None, payload)

        resp = self.oauth.api_post("/trade/v2/orders", json=payload)
        if 200 <= resp.status_code < 300:
            try:
                data = resp.json() if resp.text else {}
            except Exception:
                data = {}
            order_id = None
            if isinstance(data, dict):
                order_id = data.get("OrderId") or data.get("OrderId".lower())
            return SaxoResult(True, order_id, None, data)
        if resp.status_code == 401:
            return SaxoResult(False, None, "401 Unauthorized: check token scope.", None)
        if resp.status_code == 403:
            return SaxoResult(False, None, "403 Forbidden: insufficient privileges or wrong environment.", None)
        if resp.status_code == 404:
            return SaxoResult(False, None, "404 Not Found: verify endpoint or account.", None)
        return SaxoResult(False, None, f"Unexpected status {resp.status_code}: {resp.text}", None)


def _load_default_uic() -> str:
    uic = os.getenv(DEFAULT_USDJPY_UIC_ENV)
    if not uic:
        raise RuntimeError(
            "Set SAXO_USDJPY_UIC environment variable with the Saxo UIC for USDJPY (FxSpot)."
        )
    return uic


def _extract_equity(data: Dict[str, Any]) -> Optional[float]:
    if not isinstance(data, dict):
        return None

    # Common shapes: {Balance:{TotalValue:...}} or {TotalValue:...}
    candidates = [
        (data.get("Balance"), ["TotalValue", "AccountValue", "CashBalance", "Balance"]),
        (data, ["TotalValue", "TotalEquity", "NetEquity", "AccountValue", "CashBalance"]),
    ]
    for obj, keys in candidates:
        if isinstance(obj, dict):
            for key in keys:
                val = obj.get(key)
                if isinstance(val, (int, float)):
                    return float(val)
                if isinstance(val, dict):
                    for sub in ("Value", "Amount"):
                        if isinstance(val.get(sub), (int, float)):
                            return float(val[sub])

    balances = data.get("Balances")
    if isinstance(balances, list) and balances:
        for item in balances:
            if isinstance(item, dict):
                val = _extract_equity(item)
                if val is not None:
                    return val
    return None


def _extract_positions(payload: Dict[str, Any]) -> Dict[int, int]:
    positions: Dict[int, int] = {}
    items = None
    if isinstance(payload, dict):
        for key in ("Data", "Positions"):
            if isinstance(payload.get(key), list):
                items = payload[key]
                break
    if not items:
        return positions

    for item in items:
        if not isinstance(item, dict):
            continue
        pos = item.get("PositionBase") or item.get("Position") or item
        uic = pos.get("Uic") or (pos.get("Instrument") or {}).get("Uic")
        amount = pos.get("Amount")
        if uic is None or amount is None:
            continue
        try:
            positions[int(uic)] = int(amount)
        except Exception:
            continue
    return positions


def _extract_margin_required(data: Dict[str, Any]) -> Optional[float]:
    if not isinstance(data, dict):
        return None
    for key in ("MarginRequired", "Margin", "RequiredMargin"):
        val = data.get(key)
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, dict):
            for sub in ("Value", "Amount"):
                if isinstance(val.get(sub), (int, float)):
                    return float(val[sub])
    return None
