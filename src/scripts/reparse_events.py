from src.process_content import classify_and_parse
from src.storage import DB_PATH, connect_db
from src import runtime_config


REQUIRED_FIELDS = (
    "action",
    "instrument",
    "uic",
    "asset_type",
    "signal_timestamp",
)


def _load_uic_map() -> dict:
    settings = runtime_config.load_settings()
    raw = settings.get("saxo_uic_map") or {}
    return {str(k).upper().replace("/", ""): int(v) for k, v in raw.items()}


def main() -> None:
    uic_map = _load_uic_map()
    conn = connect_db(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT * FROM parsed_events WHERE is_trading = 1 ORDER BY id DESC")
    rows = [dict(r) for r in cur.fetchall()]

    updated = 0
    skipped = 0
    for row in rows:
        parsed = classify_and_parse(row["segment_text"], row["scraped_at"], uic_map)
        if not parsed.get("is_trading"):
            skipped += 1
            continue
        cur.execute(
            """
            UPDATE parsed_events
            SET
                pair = ?,
                action = ?,
                side = ?,
                lot_ratio = ?,
                is_add = ?,
                entry_price = ?,
                sl_price = ?,
                tp_price = ?,
                signal_id = ?,
                direction = ?,
                instrument = ?,
                uic = ?,
                asset_type = ?,
                signal_timestamp = ?,
                segment_text = ?
            WHERE id = ?
            """,
            (
                parsed.get("pair"),
                parsed.get("action"),
                parsed.get("side"),
                parsed.get("lot_ratio"),
                1 if parsed.get("is_add") else 0,
                parsed.get("entry_price"),
                parsed.get("sl_price"),
                parsed.get("tp_price"),
                parsed.get("signal_id"),
                parsed.get("direction"),
                parsed.get("instrument"),
                parsed.get("uic"),
                parsed.get("asset_type"),
                parsed.get("signal_timestamp"),
                parsed.get("segment_text"),
                row["id"],
            ),
        )
        updated += 1

    conn.commit()
    conn.close()

    print(f"Reparsed: {updated}")
    print(f"Skipped: {skipped}")


if __name__ == "__main__":
    main()
