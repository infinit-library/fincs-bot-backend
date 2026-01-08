import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .broker import BrokerResult, get_broker
from src import runtime_config
from .storage import (
    DB_PATH,
    clear_baseline_units,
    connect_db,
    get_all_trading_events,
    get_baseline_units,
    get_daily_equity,
    get_recent_executions,
    list_executions,
    record_execution,
    set_baseline_units,
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


def _find_max_units(broker, uic: int, direction: str, equity: float, max_units: int) -> Optional[int]:
    if max_units <= 0:
        return None

    high = max_units
    low = 0
    last_ok = None

    # Quick check at max_units
    margin = broker.precheck_order(uic, direction, high) if hasattr(broker, "precheck_order") else None
    if margin is not None and margin <= equity:
        return high

    while low <= high:
        mid = (low + high) // 2
        if mid == 0:
            return last_ok
        margin = broker.precheck_order(uic, direction, mid) if hasattr(broker, "precheck_order") else None
        if margin is None:
            return last_ok
        if margin <= equity:
            last_ok = mid
            low = mid + 1
        else:
            high = mid - 1

    return last_ok


def execute_pending_signals(
    broker_name: str,
    dry_run: bool = True,
    allowed_pairs: Optional[List[str]] = None,
    max_total_units: int = runtime_config.DEFAULT_SETTINGS.get("max_total_units", 500000),
    freshness_seconds: int = runtime_config.DEFAULT_SETTINGS.get("signal_freshness_seconds", 180),
    process_last_n: int = runtime_config.DEFAULT_SETTINGS.get("process_last_n", 0),
    strict_mode: bool = runtime_config.DEFAULT_SETTINGS.get("strict_mode", True),
    allow_market_without_prices: bool = runtime_config.DEFAULT_SETTINGS.get("allow_market_without_prices", False),
) -> Dict[str, Any]:
    if os.getenv("SAXO_ENV", "").lower() != "sim":
        _stop("SAXO_ENV must be sim")
    if os.getenv("BOT_ENABLED", "") != "true":
        _stop("BOT_ENABLED is not true")

    broker = get_broker(broker_name)
    if getattr(broker, "name", "") != "saxo":
        _stop("Unsupported broker")


    log_skips = os.getenv("EXECUTOR_LOG_SKIPS", "").lower() == "true"
    skip_reasons: Dict[str, int] = {}

    def _note_skip(reason: str, sig: Optional[Dict[str, Any]] = None) -> None:
        skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        if log_skips:
            hash_prefix = ""
            if sig:
                hash_prefix = str(sig.get("segment_hash", ""))[:8]
            if hash_prefix:
                print(f"SKIP[{reason}] {hash_prefix}")
            else:
                print(f"SKIP[{reason}]")

    conn = connect_db(DB_PATH)
    all_signals = get_all_trading_events(conn, limit=500)

    if process_last_n and process_last_n > 0:
        signals = all_signals[:process_last_n]
    else:
        latest_scrape = None
        if all_signals:
            latest_scrape = all_signals[0].get("scraped_at")
        signals = [s for s in all_signals if s.get("scraped_at") == latest_scrape]

    if log_skips:
        print(f"Signals loaded: {len(signals)} (process_last_n={process_last_n})")

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
            _note_skip("duplicate", sig)
            continue
        if was_executed_recent(conn, segment_hash, broker.name, window_seconds=600):
            _note_skip("duplicate_recent", sig)
            continue

        action = sig.get("action")
        direction = sig.get("direction")
        instrument = sig.get("instrument")
        uic = sig.get("uic")
        asset_type = sig.get("asset_type")
        signal_timestamp = sig.get("signal_timestamp")
        lot_ratio = sig.get("lot_ratio")
        is_add = bool(sig.get("is_add"))

        if not (action and instrument and uic is not None and asset_type and signal_timestamp):
            _note_skip("missing_required_fields", sig)
            continue
        if action not in ("ENTRY", "CLOSE_TP", "CLOSE_SL"):
            _note_skip("unsupported_action", sig)
            continue
        if asset_type != "FxSpot":
            _note_skip("unsupported_asset_type", sig)
            continue
        try:
            uic = int(uic)
        except Exception:
            _note_skip("invalid_uic", sig)
            continue

        norm_instrument = str(instrument).upper().replace("/", "")
        if norm_instrument not in ALLOWED_UICS:
            _note_skip("instrument_not_allowed", sig)
            continue
        if ALLOWED_UICS[norm_instrument] != uic:
            _note_skip("uic_mismatch", sig)
            continue

        ts = _parse_timestamp(signal_timestamp)
        if ts is None:
            _note_skip("invalid_timestamp", sig)
            continue
        if freshness_seconds > 0 and (datetime.now(timezone.utc) - ts).total_seconds() > freshness_seconds:
            _note_skip("stale_signal", sig)
            continue

        if allowed_pairs and norm_instrument not in allowed_pairs:
            _note_skip("pair_not_allowed", sig)
            continue

        if action == "ENTRY" and (direction not in ("BUY", "SELL") or lot_ratio is None):
            _note_skip("missing_direction_or_lot", sig)
            continue

        processed += 1

        # Close signals
        if action in ("CLOSE_TP", "CLOSE_SL"):
            existing_units = broker_positions.get(uic) if uic in broker_positions else broker.get_open_position_units(uic)
            if not existing_units:
                _note_skip("no_position", sig)
                skipped.append({"segment_hash": segment_hash, "reason": "no position"})
                record_execution(conn, segment_hash, broker.name, "skipped", error_message="no position")
                continue

            close_side = "SELL" if existing_units > 0 else "BUY"
            result: BrokerResult = broker.place_market_order(
                instrument=uic,
                side=close_side,
                units=abs(int(existing_units)),
                sl_price=None,
                tp_price=None,
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
                clear_baseline_units(conn, norm_instrument)
                order_rejections = 0
            else:
                failed.append({"segment_hash": segment_hash, "error": result.error})
                order_rejections += 1
                if order_rejections >= 3:
                    _stop("3 consecutive order rejections")
            continue

        # ENTRY signals
        baseline_units = None
        if is_add:
            baseline_units = get_baseline_units(conn, norm_instrument, direction)
            if baseline_units is None:
                _note_skip("missing_baseline", sig)
                skipped.append({"segment_hash": segment_hash, "reason": "missing baseline"})
                record_execution(conn, segment_hash, broker.name, "skipped", error_message="missing baseline")
                continue
        else:
            baseline_units = _find_max_units(broker, uic, direction, equity, max_total_units)
            if baseline_units is None:
                _note_skip("baseline_unavailable", sig)
                skipped.append({"segment_hash": segment_hash, "reason": "baseline unavailable"})
                record_execution(conn, segment_hash, broker.name, "skipped", error_message="baseline unavailable")
                continue
            set_baseline_units(conn, norm_instrument, direction, baseline_units)

        units = int(round(baseline_units * float(lot_ratio)))
        if units <= 0:
            _note_skip("zero_units", sig)
            skipped.append({"segment_hash": segment_hash, "reason": "zero units"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="zero units")
            continue

        # Max simultaneous open positions = 3
        distinct_open = [k for k, v in broker_positions.items() if v]
        if len(distinct_open) >= 3 and uic not in distinct_open:
            _note_skip("max_open_positions", sig)
            skipped.append({"segment_hash": segment_hash, "reason": "max open positions"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="max open positions")
            continue

        # Conflict rule: do not open opposite positions
        existing_units = broker_positions.get(uic) if uic in broker_positions else broker.get_open_position_units(uic)
        conflict = existing_units and (existing_units > 0 > units or existing_units < 0 < units)
        if conflict:
            _note_skip("conflict_position", sig)
            skipped.append({"segment_hash": segment_hash, "reason": "conflict position"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="conflict position")
            continue

        entry_price = sig.get("entry_price")
        sl_price = sig.get("sl_price")
        skip_risk_checks = False
        if entry_price is None or sl_price is None:
            if strict_mode or not allow_market_without_prices:
                _note_skip("missing_entry_sl", sig)
                skipped.append({"segment_hash": segment_hash, "reason": "missing entry/sl"})
                record_execution(conn, segment_hash, broker.name, "skipped", error_message="missing entry/sl")
                continue
            skip_risk_checks = True

        if not skip_risk_checks:
            try:
                risk_amount = abs(float(entry_price) - float(sl_price)) * abs(int(units))
            except Exception:
                _note_skip("risk_calc_failed", sig)
                skipped.append({"segment_hash": segment_hash, "reason": "risk calc failed"})
                record_execution(conn, segment_hash, broker.name, "skipped", error_message="risk calc failed")
                continue

            if equity <= 0:
                _stop("Invalid equity")
            if risk_amount > (equity * 0.01):
                _note_skip("risk_limit", sig)
                skipped.append({"segment_hash": segment_hash, "reason": "risk limit"})
                record_execution(conn, segment_hash, broker.name, "skipped", error_message="risk limit")
                continue

            try:
                notional = abs(float(entry_price)) * abs(int(units))
            except Exception:
                _note_skip("exposure_calc_failed", sig)
                skipped.append({"segment_hash": segment_hash, "reason": "exposure calc failed"})
                record_execution(conn, segment_hash, broker.name, "skipped", error_message="exposure calc failed")
                continue

            if notional > (equity * 0.10):
                _note_skip("exposure_limit", sig)
                skipped.append({"segment_hash": segment_hash, "reason": "exposure limit"})
                record_execution(conn, segment_hash, broker.name, "skipped", error_message="exposure limit")
                continue

        margin_required = broker.precheck_order(uic, direction, units) if hasattr(broker, "precheck_order") else None
        if margin_required is None:
            api_errors += 1
            if api_errors >= 3:
                _stop("3 consecutive Saxo API errors")
            _note_skip("margin_unavailable", sig)
            skipped.append({"segment_hash": segment_hash, "reason": "margin unavailable"})
            record_execution(conn, segment_hash, broker.name, "skipped", error_message="margin unavailable")
            continue
        api_errors = 0
        if margin_required > (equity * 0.03):
            _note_skip("margin_limit", sig)
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



    if log_skips:
        print(f"RESULT processed={processed} submitted={submitted} failed={len(failed)} skipped={len(skipped)}")
        if not skip_reasons:
            print("SKIP SUMMARY")
            print("- none")
    if skip_reasons:
        print("SKIP SUMMARY")
        for reason in sorted(skip_reasons):
            print(f"- {reason}: {skip_reasons[reason]}")
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
        allowed_pairs=settings.get("allowed_pairs"),
        max_total_units=int(settings.get("max_total_units", runtime_config.DEFAULT_SETTINGS.get("max_total_units", 500000))),
        freshness_seconds=int(settings.get("signal_freshness_seconds", runtime_config.DEFAULT_SETTINGS.get("signal_freshness_seconds", 180))),
        process_last_n=int(settings.get("process_last_n", runtime_config.DEFAULT_SETTINGS.get("process_last_n", 0))),
        strict_mode=bool(settings.get("strict_mode", runtime_config.DEFAULT_SETTINGS.get("strict_mode", True))),
        allow_market_without_prices=bool(settings.get("allow_market_without_prices", runtime_config.DEFAULT_SETTINGS.get("allow_market_without_prices", False))),
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


if __name__ == "__main__":
    run_execution_cycle()
