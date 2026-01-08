import re
from typing import Optional

from src import runtime_config
from .storage import DB_PATH, connect_db, insert_parsed_event, insert_raw_snapshot, sha256_text, utcnow


PAIR_LIST = [
    "EURUSD",
    "USDJPY",
    "GBPUSD",
    "GBPJPY",
    "EURJPY",
    "AUDUSD",
    "NZDUSD",
    "USDCAD",
    "USDCHF",
]

JP_PAIR_MAP = {
    "ドル/円": "USDJPY",
    "ドル円": "USDJPY",
    "ユーロ/ドル": "EURUSD",
    "ユーロドル": "EURUSD",
    "ポンド/ドル": "GBPUSD",
    "ポンドドル": "GBPUSD",
    "ユーロ/円": "EURJPY",
    "ユーロ円": "EURJPY",
    "ポンド/円": "GBPJPY",
    "ポンド円": "GBPJPY",
}


def split_into_segments(raw_text: str) -> list[str]:
    # Scrape output uses `---` as separator between atomic blocks.
    return [s.strip() for s in raw_text.split("---") if s.strip()]


def _normalize_digits(s: str) -> str:
    trans = str.maketrans("０１２３４５６７８９％", "0123456789%")
    return s.translate(trans)


def _extract_signal_line(seg: str) -> Optional[str]:
    arrow = "→"
    keywords = ("エントリー", "利確", "損切り", "ロング", "ショート", "買い", "売り")
    for line in (seg or "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if arrow not in stripped and "->" not in stripped:
            continue
        if any(k in stripped for k in keywords):
            return stripped
    return None


def _parse_pair(text: str) -> Optional[str]:
    for p in PAIR_LIST:
        if re.search(rf"\b{p}\b", text, flags=re.IGNORECASE):
            return p
    for jp, sym in JP_PAIR_MAP.items():
        if jp in text:
            return sym
    return None


def _parse_direction(text: str) -> Optional[str]:
    if re.search(r"\b(SELL|SHORT)\b", text, re.IGNORECASE) or "ショート" in text or "売り" in text:
        return "SELL"
    if re.search(r"\b(BUY|LONG)\b", text, re.IGNORECASE) or "ロング" in text or "買い" in text:
        return "BUY"
    return None


def _parse_action(text: str) -> Optional[str]:
    if "利確" in text:
        return "CLOSE_TP"
    if "損切り" in text:
        return "CLOSE_SL"
    if "エントリー" in text or re.search(r"\bENTRY\b", text, re.IGNORECASE):
        return "ENTRY"
    return None


def _parse_lot_ratio(text: str) -> Optional[float]:
    norm = _normalize_digits(text)
    m = re.search(r"最大ロットの\s*([0-9]+(?:\.[0-9]+)?)\s*割", norm)
    if m:
        try:
            return float(m.group(1)) / 10.0
        except Exception:
            return None
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*%", norm)
    if m:
        try:
            return float(m.group(1)) / 100.0
        except Exception:
            return None
    return None


def _is_add(text: str) -> bool:
    return "(追加)" in text or "（追加）" in text


def _load_uic_map() -> dict:
    settings = runtime_config.load_settings()
    raw = settings.get("saxo_uic_map") or {}
    return {str(k).upper().replace("/", ""): int(v) for k, v in raw.items()}


def classify_and_parse(seg: str, scraped_at: str, uic_map: dict) -> dict:
    line = _extract_signal_line(seg)
    if not line:
        return {"is_trading": False}

    pair = _parse_pair(line)
    instrument = pair
    direction = _parse_direction(line)
    action = _parse_action(line)
    lot_ratio = _parse_lot_ratio(line)
    is_add = _is_add(line)

    if not action or not instrument:
        return {"is_trading": False}

    norm_instrument = instrument.upper().replace("/", "")
    uic = uic_map.get(norm_instrument)
    if uic is None:
        return {"is_trading": False}

    if action == "ENTRY" and (direction is None or lot_ratio is None):
        return {"is_trading": False}

    side = "LONG" if direction == "BUY" else "SHORT" if direction == "SELL" else None

    return {
        "is_trading": True,
        "pair": norm_instrument,
        "action": action,
        "side": side,
        "lot_ratio": lot_ratio,
        "is_add": is_add,
        "entry_price": None,
        "sl_price": None,
        "tp_price": None,
        "signal_id": None,
        "direction": direction,
        "instrument": norm_instrument,
        "uic": uic,
        "asset_type": "FxSpot",
        "signal_timestamp": scraped_at,
        "segment_text": line,
    }


def save_snapshot_and_segments(raw_text: str, channel: str = "NOBU_CHANNEL"):
    scraped_at = utcnow()
    uic_map = _load_uic_map()

    conn = connect_db(DB_PATH)

    segments = split_into_segments(raw_text)
    trading_segments = []
    for seg in segments:
        parsed = classify_and_parse(seg, scraped_at, uic_map)
        if parsed.get("is_trading"):
            trading_segments.append((seg, parsed))

    if not trading_segments:
        conn.close()
        return {
            "segments_total": len(segments),
            "inserted": 0,
            "inserted_trading": 0,
        }

    raw_hash = sha256_text(raw_text)
    insert_raw_snapshot(
        conn,
        scraped_at=scraped_at,
        channel=channel,
        raw_hash=raw_hash,
        raw_text=raw_text,
    )

    inserted = 0
    inserted_trading = 0
    for seg, parsed in trading_segments:
        seg_hash = sha256_text(seg)
        did_insert = insert_parsed_event(
            conn,
            scraped_at=scraped_at,
            segment_hash=seg_hash,
            segment_text=parsed.get("segment_text") or seg,
            is_trading=parsed["is_trading"],
            pair=parsed.get("pair"),
            action=parsed.get("action"),
            side=parsed.get("side"),
            lot_ratio=parsed.get("lot_ratio"),
            is_add=parsed.get("is_add"),
            entry_price=parsed.get("entry_price"),
            sl_price=parsed.get("sl_price"),
            tp_price=parsed.get("tp_price"),
            signal_id=parsed.get("signal_id"),
            direction=parsed.get("direction"),
            instrument=parsed.get("instrument"),
            uic=parsed.get("uic"),
            asset_type=parsed.get("asset_type"),
            signal_timestamp=parsed.get("signal_timestamp"),
        )
        if did_insert:
            inserted += 1
            if parsed["is_trading"]:
                inserted_trading += 1

    conn.close()

    return {
        "segments_total": len(segments),
        "inserted": inserted,
        "inserted_trading": inserted_trading,
    }
