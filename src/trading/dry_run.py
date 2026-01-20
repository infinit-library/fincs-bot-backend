import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from src.brokers.base import BaseBroker


@dataclass(frozen=True)
class TradingSignal:
    symbol: str
    side: str
    units: int


@dataclass(frozen=True)
class RiskLimits:
    max_units: int
    max_spread: float
    max_open_positions: int


@dataclass(frozen=True)
class DryRunResult:
    ok: bool
    reason: Optional[str]
    order_payload: Optional[Dict[str, Any]]
    mid: Optional[float]
    spread: Optional[float]
    open_positions: Optional[int]
    live_confirmed: bool


def load_signal_from_env() -> TradingSignal:
    symbol = os.getenv("SAXO_SIGNAL_SYMBOL", "USDJPY").strip().upper()
    side = os.getenv("SAXO_SIGNAL_SIDE", "BUY").strip().upper()
    units_raw = os.getenv("SAXO_SIGNAL_UNITS", "10000").strip()
    units = int(units_raw)
    return TradingSignal(symbol=symbol, side=side, units=units)


def load_limits_from_env() -> RiskLimits:
    max_units = int(os.getenv("SAXO_MAX_UNITS", "10000"))
    max_spread = float(os.getenv("SAXO_MAX_SPREAD", "0.05"))
    max_open_positions = int(os.getenv("SAXO_MAX_OPEN_POSITIONS", "5"))
    return RiskLimits(max_units=max_units, max_spread=max_spread, max_open_positions=max_open_positions)


def live_confirmed() -> bool:
    return os.getenv("SAXO_LIVE_CONFIRM", "").strip() == "I_UNDERSTAND"


def run_dry_run(broker: BaseBroker, signal: TradingSignal, limits: RiskLimits) -> DryRunResult:
    if signal.units <= 0:
        return DryRunResult(False, "Units must be positive", None, None, None, None, live_confirmed())

    if signal.side not in {"BUY", "SELL"}:
        return DryRunResult(False, "Signal side must be BUY or SELL", None, None, None, None, live_confirmed())

    if signal.symbol != "USDJPY":
        return DryRunResult(False, "Only USDJPY supported in this phase", None, None, None, None, live_confirmed())

    if signal.units > limits.max_units:
        return DryRunResult(False, "Signal exceeds max units limit", None, None, None, None, live_confirmed())

    price_payload = broker.get_price(signal.symbol)
    mid, spread = _extract_mid_spread(price_payload)
    if spread is None:
        return DryRunResult(False, "Unable to compute spread from price payload", None, mid, None, None, live_confirmed())

    if spread > limits.max_spread:
        return DryRunResult(False, "Spread exceeds max spread limit", None, mid, spread, None, live_confirmed())

    positions_payload = broker.get_positions()
    open_positions = _count_positions(positions_payload)
    if open_positions is not None and open_positions >= limits.max_open_positions:
        return DryRunResult(False, "Max open positions reached", None, mid, spread, open_positions, live_confirmed())

    order_payload = _build_order_payload(signal)
    return DryRunResult(True, None, order_payload, mid, spread, open_positions, live_confirmed())


def _build_order_payload(signal: TradingSignal) -> Dict[str, Any]:
    uic = os.getenv("SAXO_USDJPY_UIC", "").strip()
    return {
        "AssetType": "FxSpot",
        "Uic": int(uic) if uic else None,
        "Amount": signal.units,
        "BuySell": "Buy" if signal.side == "BUY" else "Sell",
        "OrderType": "Market",
    }


def _extract_mid_spread(payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    # Handles common shapes from /trade/v1/prices.
    if "Prices" in payload and payload["Prices"]:
        return _mid_spread_from_price(payload["Prices"][0])

    if "PriceInfos" in payload and payload["PriceInfos"]:
        price_info = payload["PriceInfos"][0].get("PriceInfo") or payload["PriceInfos"][0]
        return _mid_spread_from_price(price_info)

    if "PriceInfo" in payload:
        return _mid_spread_from_price(payload["PriceInfo"])

    return None, None


def _mid_spread_from_price(price_info: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    bid = price_info.get("Bid")
    ask = price_info.get("Ask")
    if bid is None or ask is None:
        return None, None
    mid = (float(bid) + float(ask)) / 2.0
    spread = float(ask) - float(bid)
    return mid, spread


def _count_positions(payload: Dict[str, Any]) -> Optional[int]:
    for key in ("Data", "Positions"):
        if key in payload and isinstance(payload[key], list):
            return len(payload[key])
    return None
