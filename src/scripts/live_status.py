import os
from datetime import datetime, timezone

from src.storage import connect_db, DB_PATH, get_latest_trading_event, list_executions, get_baseline_units


def _fmt_ts(value: str | None) -> str:
    if not value:
        return "N/A"
    try:
        dt = datetime.fromisoformat(value)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return value


def main() -> None:
    conn = connect_db(DB_PATH)
    latest = get_latest_trading_event(conn)
    executions = list_executions(conn, limit=1)
    conn.close()

    print("LIVE STATUS")
    print(f"BOT_ENABLED: {os.getenv('BOT_ENABLED', 'false')}")

    if latest:
        print("\nLatest signal")
        print(f"- id: {latest.get('id')}")
        print(f"- time: {_fmt_ts(latest.get('scraped_at'))}")
        print(f"- pair: {latest.get('pair')}")
        print(f"- action: {latest.get('action')}")
        print(f"- direction: {latest.get('direction')}")
        print(f"- lot_ratio: {latest.get('lot_ratio')}")
        print(f"- is_add: {bool(latest.get('is_add'))}")
        print(f"- uic: {latest.get('uic')}")
    else:
        print("\nLatest signal: N/A")

    if executions:
        last_exec = executions[0]
        print("\nLast execution")
        print(f"- time: {_fmt_ts(last_exec.get('created_at'))}")
        print(f"- broker: {last_exec.get('broker')}")
        print(f"- status: {last_exec.get('status')}")
        print(f"- error: {last_exec.get('error_message') or 'N/A'}")
    else:
        print("\nLast execution: N/A")

    conn = connect_db(DB_PATH)
    baseline_buy = get_baseline_units(conn, "USDJPY", "BUY")
    baseline_sell = get_baseline_units(conn, "USDJPY", "SELL")
    conn.close()
    print("\nBaseline USDJPY")
    print(f"- BUY: {baseline_buy if baseline_buy is not None else 'N/A'}")
    print(f"- SELL: {baseline_sell if baseline_sell is not None else 'N/A'}")


if __name__ == "__main__":
    main()
