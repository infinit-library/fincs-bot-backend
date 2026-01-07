import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .broker import BrokerResult, get_broker
from src import runtime_config
from .storage import (
    DB_PATH,
    connect_db,
    get_all_trading_events,
    get_daily_equity,
    get_recent_executions,
    list_executions,
    record_execution,
    set_daily_equity,
    was_executed,
    was_executed_recent,
)


ALLOWED_UICS = {
    "EURUSD": 21,
    "USDJPY": 22,
    "GBPUSD": 23,
}


def _stop(reason: str) -> None:
    print(f"STOP: {reason}")
    raise SystemExit(1)


def _compute_units(side: str, lot_ratio: Optional[float], base_units_per_lot: int, max_lot_cap: float) -> int:
    ratio = lot_ratio if lot_ratio is not None else 1.0
    ratio = max(0.01, min(ratio, max_lot_cap))
    units = int(round(base_units_per_lot * ratio))
    if side == "SHORT":
        units = -units
    return units


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit() and len(text) == 10:
        try:
            return datetime.fromtimestamp(int(text), tz=timezone.utc)
        except Exception:
            return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def execute_pending_signals(
    broker_name: str,
    dry_run: bool = True,
    base_units_per_lot: int = runtime_config.DEFAULT_SETTINGS["base_units_per_lot"],
    max_lot_cap: float = runtime_config.DEFAULT_SETTINGS["max_lot_cap"],
    allowed_pairs: Optional[List[str]] = None,
) -> Dict[str, Any]:
    if os.getenv("SAXO_ENV", "").lower() != "sim":
        _stop("SAXO_ENV must be sim")
    if os.getenv("BOT_ENABLED", "") != "true":
        _stop("BOT_ENABLED is not true")

    broker = get_broker(broker_name)
    if getattr(broker, "name", "") != "saxo":
        _stop("Unsupported broker")

    conn = connect_db(DB_PATH)
    all_signals = get_all_trading_events(conn, limit=500)

    latest_scrape = None
    if all_signals:
        latest_scrape = all_signals[0].get("scraped_at")
    signals = [s for s in all_signals if s.get("scraped_at") == latest_scrape]

    broker_positions = {}
    try:
        broker_positions = broker.refresh_positions() if hasattr(broker, "refresh_positions") else {}
    except Exception:
        broker_positions = {}

    equity = broker.get_equity() if hasattr(broker, "get_equity") else None
    if equity is None:
        _stop("Equity unavailable")

    date_key = datetime.now(timezone.utc).date().isoformat()
    baseline = get_daily_equity(conn, date_key)
    if baseline is None:
        set_daily_equity(conn, date_key, equity)
        baseline = equity
    if baseline and baseline > 0:
        drawdown = (baseline - equity) / baseline
        if drawdown >= 0.05:
            _stop("Daily drawdown limit reached")

    recent = get_recent_executions(conn, broker.name, limit=3)
    if len(recent) == 3 and all(r.get("status") == "failed" for r in recent):
        _stop("3 consecutive broker failures")

    processed = 0
    submitted = 0
    failed: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    api_errors = 0
    order_rejections = 0

    for sig in signals:
        segment_hash = sig["segment_hash"]
        signal_id = sig.get("signal_id") or segment_hash[:24]

        if was_executed(conn, segment_hash, broker.name):
            continue
        if was_executed_recent(conn, segment_hash, broker.name, window_seconds=600):
            continue

        direction = sig.get("direction")
        instrument = sig.get("instrument")
        uic = sig.get("uic")
        asset_type = sig.get("asset_type")
        signal_timestamp = sig.get("signal_timestamp")

        if not (direction and instrument and uic is not None and asset_type and signal_timestamp):
            continue
        if direction not in ("BUY", "SELL"):
            continue
        if asset_type != "FxSpot":
            continue
        try:
            uic = int(uic)
        except Exception:
            continue

        norm_instrument = str(instrument).upper().replace("/", "")
        if norm_instrument not in ALLOWED_UICS:
            continue
        if ALLOWED_UICS[norm_instrument] != uic:
            continue

        ts = _parse_timestamp(signal_timestamp)
        if ts is None:
            continue
        if (datetime.now(timezone.utc) - ts).total_seconds() > 180:
            continue

        if allowed_pairs and norm_instrument not in allowed_pairs:
            continue

        processed += 1

        # Max simultaneous open positions = 3
        distinct_open = [k for k, v in broker_positions.items() if v]
        if len(distinct_open) >= 3 and uic not in distinct_open:
            skipped.append({"segment_hash": segment_hash, "reason": "max open positions"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="max open positions")
            continue

        entry_price = sig.get("entry_price")
        sl_price = sig.get("sl_price")
        if entry_price is None or sl_price is None:
            skipped.append({"segment_hash": segment_hash, "reason": "missing entry/sl"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="missing entry/sl")
            continue

        side = "LONG" if direction == "BUY" else "SHORT"
        units = _compute_units(side, sig.get("lot_ratio"), base_units_per_lot, max_lot_cap)

        # Conflict rule: do not open opposite positions
        existing_units = broker_positions.get(uic) if uic in broker_positions else broker.get_open_position_units(uic)
        conflict = existing_units and (existing_units > 0 > units or existing_units < 0 < units)
        if conflict:
            skipped.append({"segment_hash": segment_hash, "reason": "conflict position"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="conflict position")
            continue

        try:
            risk_amount = abs(float(entry_price) - float(sl_price)) * abs(int(units))
        except Exception:
            skipped.append({"segment_hash": segment_hash, "reason": "risk calc failed"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="risk calc failed")
            continue

        if equity <= 0:
            _stop("Invalid equity")
        if risk_amount > (equity * 0.01):
            skipped.append({"segment_hash": segment_hash, "reason": "risk limit"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="risk limit")
            continue

        try:
            notional = abs(float(entry_price)) * abs(int(units))
        except Exception:
            skipped.append({"segment_hash": segment_hash, "reason": "exposure calc failed"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="exposure calc failed")
            continue

        if notional > (equity * 0.10):
            skipped.append({"segment_hash": segment_hash, "reason": "exposure limit"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="exposure limit")
            continue

        margin_required = broker.precheck_order(uic, direction, units) if hasattr(broker, "precheck_order") else None
        if margin_required is None:
            api_errors += 1
            if api_errors >= 3:
                _stop("3 consecutive Saxo API errors")
            skipped.append({"segment_hash": segment_hash, "reason": "margin unavailable"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="margin unavailable")
            continue
        api_errors = 0
        if margin_required > (equity * 0.03):
            skipped.append({"segment_hash": segment_hash, "reason": "margin limit"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="margin limit")
            continue

        result: BrokerResult = broker.place_market_order(
            instrument=uic,
            side=direction,
            units=units,
            sl_price=sig.get("sl_price"),
            tp_price=sig.get("tp_price"),
            client_id=signal_id,
            dry_run=dry_run,
        )

        status = "filled" if result.ok else "failed"
        record_execution(
            conn,
            segment_hash,
            broker.name,
            status,
            order_id=result.order_id,
            error_message=result.error,
            payload=json.dumps(result.payload) if result.payload else None,
        )

        if result.ok:
            submitted += 1
            order_rejections = 0
        else:
            failed.append({"segment_hash": segment_hash, "error": result.error})
            order_rejections += 1
            if order_rejections >= 3:
                _stop("3 consecutive order rejections")

    conn.close()
    return {
        "processed": processed,
        "submitted": submitted,
        "failed": failed,
        "skipped": skipped,
        "dry_run": dry_run,
        "broker": broker.name,
    }


def run_execution_cycle() -> Dict[str, Any]:
    settings = runtime_config.load_settings()
    return execute_pending_signals(
        broker_name=settings.get("broker", runtime_config.DEFAULT_SETTINGS["broker"]),
        dry_run=bool(settings.get("dry_run", True)),
        base_units_per_lot=int(settings.get("base_units_per_lot", runtime_config.DEFAULT_SETTINGS["base_units_per_lot"])),
        max_lot_cap=float(settings.get("max_lot_cap", runtime_config.DEFAULT_SETTINGS["max_lot_cap"])),
        allowed_pairs=settings.get("allowed_pairs"),
    )


def run_loop(poll_interval: int = runtime_config.DEFAULT_SETTINGS["poll_interval"]) -> None:
    poll_interval = max(5, int(poll_interval))
    while True:
        try:
            run_execution_cycle()
        except SystemExit:
            raise
        except Exception:
            pass
        time.sleep(poll_interval)


def list_recent_orders(limit: int = 100) -> List[Dict[str, Any]]:
    conn = connect_db(DB_PATH)
    rows = list_executions(conn, limit=limit)
    conn.close()
    return rows
