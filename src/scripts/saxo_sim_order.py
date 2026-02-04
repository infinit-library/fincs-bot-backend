import os

from src.broker import get_broker


def main() -> None:
    broker = get_broker("saxo")
    env = os.getenv("SAXO_ENV", "sim").lower()
    if env not in {"sim", "live"}:
        raise SystemExit("SAXO_ENV must be 'sim' or 'live'.")

    if env == "sim":
        confirm = os.getenv("SAXO_SIM_CONFIRM", "").strip()
        if confirm != "I_UNDERSTAND":
            raise SystemExit("Set SAXO_SIM_CONFIRM=I_UNDERSTAND to place a SIM order.")
    else:
        confirm = os.getenv("SAXO_LIVE_CONFIRM", "").strip()
        if confirm != "I_UNDERSTAND":
            raise SystemExit("Set SAXO_LIVE_CONFIRM=I_UNDERSTAND to place a LIVE order.")

    pair = os.getenv("SAXO_TEST_PAIR", "USDJPY")
    side = os.getenv("SAXO_TEST_SIDE", "BUY")
    units = int(os.getenv("SAXO_TEST_UNITS", "10000"))

    result = broker.place_market_order(
        instrument=pair,
        side=side,
        units=units,
        dry_run=False,
    )

    print("Result ok:", result.ok)
    print("Order ID:", result.order_id)
    print("Error:", result.error)
    print("Payload:", result.payload)


if __name__ == "__main__":
    main()
