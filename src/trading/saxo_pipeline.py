import json
import os
from typing import Any, Dict, Optional, Tuple

from src import runtime_config
from src.brokers.saxo import SaxoBroker
from src.storage import (
    DB_PATH,
    connect_db,
    get_latest_trading_event,
    record_execution,
    record_trade_audit,
    was_executed,
)
from src.trading.dry_run import TradingSignal, load_limits_from_env, run_dry_run, live_confirmed


BROKER_NAME = "saxo"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def _map_side(raw_side: Optional[str]) -> Optional[str]:
    if not raw_side:
        return None
    side = raw_side.strip().upper()
    if side in {"BUY", "LONG"}:
        return "BUY"
    if side in {"SELL", "SHORT"}:
        return "SELL"
    return None


def _build_signal_from_event(event: Dict[str, Any]) -> Tuple[Optional[TradingSignal], Optional[str]]:
    action = (event.get("action") or "").upper()
    if action and action != "ENTRY":
        return None, "not an entry signal"

    pair = (event.get("pair") or "").upper()
    if pair != "USDJPY":
        return None, "unsupported pair"

    side = _map_side(event.get("side"))
    if not side:
        return None, "missing side"

    base_units = int(os.getenv("SAXO_UNITS_PER_LOT", "10000"))
    max_lot_ratio = float(os.getenv("SAXO_MAX_LOT_RATIO", "2.0"))
    ratio = event.get("lot_ratio") or 1.0
    try:
        ratio = float(ratio)
    except Exception:
        ratio = 1.0
    ratio = max(0.01, min(ratio, max_lot_ratio))
    units = int(round(base_units * ratio))
    settings = runtime_config.load_settings()
    try:
        max_lot_cap = float(settings.get("max_lot_cap", runtime_config.DEFAULT_SETTINGS.get("max_lot_cap", 0.8)))
    except Exception:
        max_lot_cap = 0.8
    if max_lot_cap <= 0:
        max_lot_cap = 0.8
    if max_lot_cap > 1.0:
        max_lot_cap = 1.0
    max_units_cap = int(round(base_units * max_lot_cap))
    units = min(units, max_units_cap)

    return TradingSignal(symbol=pair, side=side, units=units), None


def _load_account_keys() -> Tuple[str, str]:
    account_key = os.getenv("SAXO_ACCOUNT_KEY", "").strip()
    client_key = os.getenv("SAXO_CLIENT_KEY", "").strip()
    if not account_key or not client_key:
        raise RuntimeError("Set SAXO_ACCOUNT_KEY and SAXO_CLIENT_KEY in .env")
    return account_key, client_key


def run_latest_signal_pipeline(broker: SaxoBroker) -> Dict[str, Any]:
    dry_run = _env_bool("SAXO_DRY_RUN", True)
    settings_env = os.getenv("SAXO_ENV", "sim").strip().lower()

    conn = connect_db(DB_PATH)
    event = get_latest_trading_event(conn)
    if not event:
        conn.close()
        return {"status": "no_signal"}

    segment_hash = event.get("segment_hash")
    if segment_hash and was_executed(conn, segment_hash, BROKER_NAME):
        conn.close()
        return {"status": "already_executed", "segment_hash": segment_hash}

    signal, reason = _build_signal_from_event(event)
    if not signal:
        record_trade_audit(
            conn,
            segment_hash=segment_hash,
            broker=BROKER_NAME,
            pair=event.get("pair"),
            action=event.get("action"),
            side=event.get("side"),
            dry_run=True,
            ok=False,
            reason=reason,
            mid=None,
            spread=None,
            payload=None,
        )
        record_execution(conn, segment_hash or "", BROKER_NAME, "skipped", error_message=reason)
        conn.close()
        return {"status": "skipped", "reason": reason}

    limits = load_limits_from_env()
    result = run_dry_run(broker, signal, limits)

    payload_text = None
    if result.order_payload:
        payload_text = json.dumps(result.order_payload, ensure_ascii=True)

    record_trade_audit(
        conn,
        segment_hash=segment_hash,
        broker=BROKER_NAME,
        pair=event.get("pair"),
        action=event.get("action"),
        side=event.get("side"),
        dry_run=dry_run,
        ok=result.ok,
        reason=result.reason,
        mid=result.mid,
        spread=result.spread,
        payload=payload_text,
    )

    if not result.ok:
        record_execution(conn, segment_hash or "", BROKER_NAME, "skipped", error_message=result.reason)
        conn.close()
        return {"status": "blocked", "reason": result.reason}

    if dry_run:
        record_execution(conn, segment_hash or "", BROKER_NAME, "dry_run")
        conn.close()
        return {"status": "dry_run", "signal": signal, "payload": result.order_payload}

    if settings_env == "live":
        if not _env_bool("ALLOW_LIVE_TRADING", False):
            record_execution(conn, segment_hash or "", BROKER_NAME, "blocked", error_message="live trading disabled")
            conn.close()
            return {"status": "blocked", "reason": "live trading disabled"}
        if not live_confirmed():
            record_execution(conn, segment_hash or "", BROKER_NAME, "blocked", error_message="live confirm missing")
            conn.close()
            return {"status": "blocked", "reason": "live confirm missing"}

    account_key, client_key = _load_account_keys()
    order_payload = dict(result.order_payload or {})
    order_payload.update(
        {
            "AccountKey": account_key,
            "ClientKey": client_key,
            "ManualOrder": True,
        }
    )

    submit = broker.place_market_order(order_payload)
    if submit.get("ok"):
        payload = submit.get("payload") or {}
        order_id = None
        if isinstance(payload, dict):
            order_id = payload.get("OrderId") or payload.get("orderId")
        record_execution(
            conn,
            segment_hash or "",
            BROKER_NAME,
            "submitted",
            order_id=str(order_id) if order_id else None,
            payload=json.dumps(payload, ensure_ascii=True),
        )
        conn.close()
        return {"status": "submitted", "payload": submit.get("payload"), "order_id": order_id}

    error = submit.get("error") or "order submission failed"
    record_execution(conn, segment_hash or "", BROKER_NAME, "failed", error_message=error)
    conn.close()
    return {"status": "failed", "error": error}


if __name__ == "__main__":
    raise SystemExit("Use src.main to run the pipeline.")
