import json
from typing import Any, Dict, List, Optional

from .broker import BrokerResult, get_broker, to_oanda_instrument
from .config import DEFAULT_SETTINGS, load_settings
from .storage import (
    DB_PATH,
    connect_db,
    get_all_trading_events,
    list_executions,
    record_execution,
    was_executed,
)
import time


def _compute_units(side: str, lot_ratio: Optional[float], base_units_per_lot: int, max_lot_cap: float) -> int:
    ratio = lot_ratio if lot_ratio is not None else 1.0
    ratio = max(0.01, min(ratio, max_lot_cap))
    units = int(round(base_units_per_lot * ratio))
    if side == "SHORT":
        units = -units
    return units


def execute_pending_signals(
    broker_name: str,
    dry_run: bool = True,
    base_units_per_lot: int = DEFAULT_SETTINGS["base_units_per_lot"],
    max_lot_cap: float = DEFAULT_SETTINGS["max_lot_cap"],
    allowed_pairs: Optional[List[str]] = None,
    max_slippage: float = DEFAULT_SETTINGS["max_slippage"],
    max_spread: float = DEFAULT_SETTINGS.get("max_spread", 0.0008),
    price_retries: int = DEFAULT_SETTINGS.get("price_retries", 2),
    conflict_policy: str = DEFAULT_SETTINGS.get("conflict_policy", "skip"),
    max_open_positions: int = DEFAULT_SETTINGS.get("max_open_positions", 5),
    max_total_units: int = DEFAULT_SETTINGS.get("max_total_units", 500000),
) -> Dict[str, Any]:
    broker = get_broker(broker_name)
    conn = connect_db(DB_PATH)
    all_signals = get_all_trading_events(conn, limit=500)  # newest first

    # Only consider the most recent scraped batch
    latest_scrape = None
    if all_signals:
        latest_scrape = all_signals[0].get("scraped_at")
    signals = [s for s in all_signals if s.get("scraped_at") == latest_scrape]

    # Cache positions once to reduce API calls
    broker_positions = {}
    try:
        broker_positions = broker.refresh_positions() if hasattr(broker, "refresh_positions") else {}
    except Exception:
        broker_positions = {}

    processed = 0
    submitted = 0
    failed: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    for sig in signals:
        segment_hash = sig["segment_hash"]
        pair = sig.get("pair")
        side = sig.get("side")
        lot_ratio = sig.get("lot_ratio")
        sl_price = sig.get("sl_price")
        tp_price = sig.get("tp_price")
        signal_id = sig.get("signal_id") or segment_hash[:24]
        entry_price = sig.get("entry_price")

        if was_executed(conn, segment_hash, broker.name):
            continue

        processed += 1

        # Guard: max open positions (distinct instruments)
        if max_open_positions is not None and max_open_positions > 0:
            distinct_open = [k for k, v in broker_positions.items() if v]
            if len(distinct_open) >= max_open_positions and pair not in distinct_open:
                skipped.append({"segment_hash": segment_hash, "reason": "max open positions"})
                record_execution(conn, segment_hash, broker.name, "skipped", error_message="max open positions")
                continue

        if allowed_pairs and pair and pair not in allowed_pairs:
            skipped.append({"segment_hash": segment_hash, "reason": "pair not allowed"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="pair not allowed")
            continue

        if not pair or not side:
            skipped.append({"segment_hash": segment_hash, "reason": "missing pair/side"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="missing pair/side")
            continue

        try:
            units = _compute_units(side, lot_ratio, base_units_per_lot, max_lot_cap)
            instrument = to_oanda_instrument(pair)

            # Conflict rule
            existing_units = broker_positions.get(instrument) if instrument in broker_positions else broker.get_open_position_units(instrument)
            conflict = existing_units and (existing_units > 0 > units or existing_units < 0 < units)
            if conflict:
                if conflict_policy == "skip":
                    skipped.append({"segment_hash": segment_hash, "reason": "conflict position"})
                    record_execution(conn, segment_hash, broker.name, "skipped", error_message="conflict position")
                    continue
                elif conflict_policy == "close_then_open":
                    # attempt to close current position first
                    close_units = -existing_units
                    close_res = broker.place_market_order(
                        instrument=instrument,
                        side="SELL" if close_units > 0 else "BUY",
                        units=abs(close_units),
                        dry_run=dry_run,
                    )
                    if not close_res.ok:
                        failed.append({"segment_hash": segment_hash, "error": f"close failed: {close_res.error}"})
                        record_execution(
                            conn,
                            segment_hash,
                            broker.name,
                            "failed",
                            error_message=f"close failed: {close_res.error}",
                        )
                        continue
                else:
                    skipped.append({"segment_hash": segment_hash, "reason": "conflict policy unsupported"})
                    record_execution(conn, segment_hash, broker.name, "skipped", error_message="conflict policy")
                    continue

            # Live price guard if entry_price provided
            if entry_price is not None:
                attempt = 0
                price_ok = False
                mid = None
                spread = None
                last_err = None
                while attempt <= price_retries and not price_ok:
                    price_result = broker.get_price(instrument)
                    if price_result.ok and price_result.payload:
                        mid = float(price_result.payload["mid"])
                        spread = float(price_result.payload.get("spread", 0.0))
                        if spread and spread > max_spread:
                            last_err = f"spread too wide: {spread}"
                        elif abs(mid - float(entry_price)) > max_slippage:
                            last_err = "price out of range"
                        else:
                            price_ok = True
                            break
                    else:
                        last_err = price_result.error or "price unavailable"
                    attempt += 1
                    if not price_ok:
                        time.sleep(0.3)
                if not price_ok:
                    skipped.append({"segment_hash": segment_hash, "reason": last_err, "mid": mid, "spread": spread})
                    record_execution(conn, segment_hash, broker.name, "skipped", error_message=last_err or "price check failed")
                    continue

            result: BrokerResult = broker.place_market_order(
                instrument=instrument,
                side=side,
                units=units,
                sl_price=sl_price,
                tp_price=tp_price,
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
            else:
                failed.append({"segment_hash": segment_hash, "error": result.error})
        except Exception as exc:
            failed.append({"segment_hash": segment_hash, "error": str(exc)})
            record_execution(conn, segment_hash, broker.name, "failed", error_message=str(exc))

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
    settings = load_settings()
    return execute_pending_signals(
        broker_name=settings.get("broker", DEFAULT_SETTINGS["broker"]),
        dry_run=bool(settings.get("dry_run", True)),
        base_units_per_lot=int(settings.get("base_units_per_lot", DEFAULT_SETTINGS["base_units_per_lot"])),
        max_lot_cap=float(settings.get("max_lot_cap", DEFAULT_SETTINGS["max_lot_cap"])),
        allowed_pairs=settings.get("allowed_pairs"),
        max_slippage=float(settings.get("max_slippage", DEFAULT_SETTINGS["max_slippage"])),
        max_spread=float(settings.get("max_spread", DEFAULT_SETTINGS["max_spread"])),
        price_retries=int(settings.get("price_retries", DEFAULT_SETTINGS["price_retries"])),
        conflict_policy=settings.get("conflict_policy", DEFAULT_SETTINGS["conflict_policy"]),
        max_open_positions=settings.get("max_open_positions", DEFAULT_SETTINGS["max_open_positions"]),
        max_total_units=settings.get("max_total_units", DEFAULT_SETTINGS["max_total_units"]),
    )


def run_loop(poll_interval: int = DEFAULT_SETTINGS["poll_interval"]) -> None:
    """
    Simple scheduler loop that runs execution every poll_interval seconds.
    Assumes scraper populates DB out-of-band.
    """
    poll_interval = max(5, int(poll_interval))
    while True:
        try:
            run_execution_cycle()
        except Exception:
            # swallow to keep loop alive; production should log
            pass
        time.sleep(poll_interval)


def list_recent_orders(limit: int = 100) -> List[Dict[str, Any]]:
    conn = connect_db(DB_PATH)
    rows = list_executions(conn, limit=limit)
    conn.close()
    return rows
