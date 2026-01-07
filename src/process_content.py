import re
from typing import Optional

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
    "XAUUSD",
    "XAGUSD",
]

# Japanese aliases to canonical symbols
JP_PAIR_MAP = {
    "ドル円": "USDJPY",
    "ドル/円": "USDJPY",
    "米ドル円": "USDJPY",
    "ユーロドル": "EURUSD",
    "ユロドル": "EURUSD",
    "ユーロ/ドル": "EURUSD",
    "ポンドドル": "GBPUSD",
    "ポン円": "GBPJPY",
    "ポンド円": "GBPJPY",
    "ユーロ円": "EURJPY",
}


def split_into_segments(raw_text: str) -> list[str]:
    # Scrape output uses `---` as separator between atomic blocks.
    return [s.strip() for s in raw_text.split("---") if s.strip()]


def _normalize_text(s: str) -> str:
    # Normalize to simplify regex parsing: upper-case, ASCII digits.
    fullwidth_digits = str.maketrans("０１２３４５６７８９．，", "0123456789.,")
    return s.translate(fullwidth_digits).upper()


def _parse_pair(text: str) -> Optional[str]:
    for p in PAIR_LIST:
        if re.search(rf"\b{p}\b", text, flags=re.IGNORECASE):
            return p
    # Japanese aliases (search on original text too)
    for jp, sym in JP_PAIR_MAP.items():
        if jp in text:
            return sym
    return None


def _parse_side(text: str) -> Optional[str]:
    if re.search(r"\b(BUY|LONG)\b", text, re.IGNORECASE):
        return "LONG"
    if re.search(r"\b(SELL|SHORT)\b", text, re.IGNORECASE):
        return "SHORT"
    if "ロング" in text or "買い" in text:
        return "LONG"
    if "ショート" in text or "売り" in text:
        return "SHORT"
    return None


def _parse_prices(text: str) -> tuple[Optional[float], Optional[float], Optional[float]]:
    entry = sl = tp = None

    def first_float(pattern: str) -> Optional[float]:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except Exception:
                return None
        return None

    entry = first_float(r"(?:ENTRY|@|INTO|AT|エントリー)\s*([0-9]+(?:\.[0-9]+)?)")
    sl = first_float(r"(?:SL|STOP(?:\s*LOSS)?|S/L|損切り|ロスカット)\s*([0-9]+(?:\.[0-9]+)?)")
    tp = first_float(r"(?:TP|TAKE\s*PROFIT|T/P|利確|利食い)\s*([0-9]+(?:\.[0-9]+)?)")

    # Fallback: if exactly three numbers appear, guess order: entry, tp, sl
    if not (entry and sl and tp):
        nums = [float(n.replace(",", "")) for n in re.findall(r"[0-9]+(?:\.[0-9]+)?", text)]
        if len(nums) == 3 and entry is None:
            entry, tp, sl = nums
    return entry, sl, tp


def _parse_lot_ratio(text: str) -> Optional[float]:
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(LOT|LOTS)", text, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def classify_and_parse(seg: str) -> dict:
    """
    Classify each atomic segment as trading/non-trading and parse fields.
    """
    s = (seg or '').strip()
    norm = _normalize_text(s)
    lower = s.lower()

    pair = _parse_pair(norm)
    side = _parse_side(norm)
    entry_price, sl_price, tp_price = _parse_prices(norm)
    lot_ratio = _parse_lot_ratio(norm)

    trading_hints = ['entry', '?????', '??', '???', 'take profit', 'tp', 'sl', 'stop', '??']
    has_hint = any(h in lower for h in trading_hints)

    # Be stricter: require a side/price/lot, or a pair plus a trading hint.
    is_trading = bool(
        side
        or entry_price
        or tp_price
        or sl_price
        or lot_ratio
        or (pair and has_hint)
    )
    action = 'ENTRY' if is_trading else None
    is_add = 'ADD' in norm or '??' in s  # lightweight add-on marker

    signal_id = None
    m_id = re.search(r"ID[:\s]*([A-Z0-9\-]+)", norm)
    if m_id:
        signal_id = m_id.group(1)

    return {
        'is_trading': is_trading,
        'pair': pair,
        'action': action,
        'side': side,
        'lot_ratio': lot_ratio,
        'is_add': is_add,
        'entry_price': entry_price,
        'sl_price': sl_price,
        'tp_price': tp_price,
        'signal_id': signal_id,
    }



def save_snapshot_and_segments(raw_text: str, channel: str = 'NOBU_CHANNEL'):
    scraped_at = utcnow()

    conn = connect_db(DB_PATH)

    # 1) Split and keep only trading-classified segments
    segments = split_into_segments(raw_text)
    trading_segments = []
    for seg in segments:
        parsed = classify_and_parse(seg)
        if parsed['is_trading']:
            trading_segments.append((seg, parsed))

    # If nothing is trading-related, skip inserts entirely.
    if not trading_segments:
        conn.close()
        return {
            'segments_total': len(segments),
            'inserted': 0,
            'inserted_trading': 0,
        }

    # 2) Save raw snapshot (dedupe by raw_hash) only when trading content exists
    raw_hash = sha256_text(raw_text)
    insert_raw_snapshot(
        conn,
        scraped_at=scraped_at,
        channel=channel,
        raw_hash=raw_hash,
        raw_text=raw_text,
    )

    # 3) Save trading segments (dedupe by segment_hash)
    inserted = 0
    inserted_trading = 0
    for seg, parsed in trading_segments:
        seg_hash = sha256_text(seg)
        did_insert = insert_parsed_event(
            conn,
            scraped_at=scraped_at,
            segment_hash=seg_hash,
            segment_text=seg,
            is_trading=parsed['is_trading'],
            pair=parsed['pair'],
            action=parsed['action'],
            side=parsed['side'],
            lot_ratio=parsed['lot_ratio'],
            is_add=parsed['is_add'],
            entry_price=parsed['entry_price'],
            sl_price=parsed['sl_price'],
            tp_price=parsed['tp_price'],
            signal_id=parsed['signal_id'],
        )
        if did_insert:
            inserted += 1
            if parsed['is_trading']:
                inserted_trading += 1

    conn.close()

    return {
        'segments_total': len(segments),
        'inserted': inserted,
        'inserted_trading': inserted_trading,
    }
