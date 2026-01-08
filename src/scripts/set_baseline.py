import sys
from typing import Optional

from src import runtime_config
from src.broker import get_broker
from src.storage import connect_db, set_baseline_units, DB_PATH


ALLOWED_DIRECTIONS = {"BUY", "SELL"}


def _find_max_units(broker, uic: int, direction: str, equity: float, max_units: int) -> Optional[int]:
    if max_units <= 0:
        return None

    high = max_units
    low = 0
    last_ok = None

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


def main() -> None:
    if len(sys.argv) < 4:
        print("Usage: python -m src.scripts.set_baseline INSTRUMENT BUY|SELL RATIO")
        raise SystemExit(1)

    instrument = sys.argv[1].upper().replace("/", "")
    direction = sys.argv[2].upper()
    ratio = float(sys.argv[3])

    if direction not in ALLOWED_DIRECTIONS:
        raise SystemExit("Direction must be BUY or SELL")
    if ratio <= 0:
        raise SystemExit("Ratio must be > 0")

    settings = runtime_config.load_settings()
    broker = get_broker(settings.get("broker", runtime_config.DEFAULT_SETTINGS["broker"]))

    uic_map = settings.get("saxo_uic_map", {})
    uic = uic_map.get(instrument)
    if not uic:
        raise SystemExit(f"Missing UIC for {instrument}")

    equity = broker.get_equity() if hasattr(broker, "get_equity") else None
    if equity is None:
        raise SystemExit("Equity unavailable")

    max_units = int(settings.get("max_total_units", runtime_config.DEFAULT_SETTINGS.get("max_total_units", 500000)))
    baseline_units = _find_max_units(broker, int(uic), direction, float(equity), max_units)
    if baseline_units is None or baseline_units <= 0:
        raise SystemExit("Baseline units unavailable")

    conn = connect_db(DB_PATH)
    set_baseline_units(conn, instrument, direction, int(baseline_units))
    conn.close()

    entry_units = int(round(baseline_units * ratio))
    print(f"Baseline set: {instrument} {direction} baseline_units={baseline_units}")
    print(f"Planned entry units at ratio {ratio}: {entry_units}")


if __name__ == "__main__":
    main()
