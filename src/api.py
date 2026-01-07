from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import threading

from .config import DEFAULT_SETTINGS, load_settings, save_settings
from .executor import list_recent_orders, run_execution_cycle
from .storage import (
    DB_PATH,
    connect_db,
    get_all_trading_events,
    get_event_statistics,
    get_latest_snapshot,
    get_recent_raw,
    get_latest_trading_event,
)
from .scheduler import run_scheduler, get_scrape_health


app = FastAPI(title="FINCS Ops API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Keep a simple runtime error note for the UI
_last_error: Optional[str] = None
_stop_event = threading.Event()
_bg_thread: Optional[threading.Thread] = None


# internal helper to run the scheduler thread once
def _start_scheduler_if_needed():
    global _bg_thread
    if _bg_thread is None or not _bg_thread.is_alive():
        _stop_event.clear()
        _bg_thread = threading.Thread(target=run_scheduler, args=(_stop_event,), daemon=True)
        _bg_thread.start()

@app.on_event("startup")
def init_db() -> None:
    conn = connect_db(DB_PATH)
    conn.close()

    # Auto-start scraping on server boot
    settings = load_settings()
    if not settings.get("running", False):
        settings["running"] = True
        save_settings(settings)
    _start_scheduler_if_needed()


@app.get("/status")
def status() -> Dict[str, Any]:
    conn = connect_db(DB_PATH)
    latest = get_latest_snapshot(conn)
    latest_signal: Optional[Dict[str, Any]] = get_latest_trading_event(conn)

    settings = load_settings()

    running = bool(settings.get("running", False))
    last_scrape = None
    last_new_segment = None
    if latest:
        last_scrape = latest.get("scraped_at")
    if latest_signal:
        last_new_segment = latest_signal.get("scraped_at")

    conn.close()

    latency_ms = None  # not tracked yet
    scrape_health = get_scrape_health()

    return {
        "running": running,
        "last_scrape": last_scrape,
        "last_new_segment": last_new_segment,
        "poll_interval": settings.get("poll_interval", 15),
        "dry_run": bool(settings.get("dry_run", True)),
        "latest_signal": latest_signal["segment_text"] if latest_signal else None,
        "latency_ms": latency_ms,
        "last_error": _last_error or scrape_health.get("last_error"),
        "scrape_last_attempt": scrape_health.get("last_attempt"),
        "scrape_last_success": scrape_health.get("last_success"),
    }


@app.post("/bot/start")
def bot_start() -> Dict[str, str]:
    settings = load_settings()
    settings["running"] = True
    save_settings(settings)
    # kick scheduler thread if not running
    global _bg_thread
    if _bg_thread is None or not _bg_thread.is_alive():
        _stop_event.clear()
        _bg_thread = threading.Thread(target=run_scheduler, args=(_stop_event,), daemon=True)
        _bg_thread.start()
    return {"status": "running"}


@app.post("/bot/stop")
def bot_stop() -> Dict[str, str]:
    settings = load_settings()
    settings["running"] = False
    save_settings(settings)
    _stop_event.set()
    return {"status": "stopped"}


@app.post("/bot/run-once")
def bot_run_once() -> Dict[str, Any]:
    global _last_error
    try:
        result = run_execution_cycle()
        _last_error = None
        return {"status": "ok", "result": result}
    except Exception as exc:
        _last_error = str(exc)
        raise


@app.get("/signals")
def list_signals(limit: int = 100) -> List[Dict[str, Any]]:
    conn = connect_db(DB_PATH)
    rows = get_all_trading_events(conn, limit=limit)
    conn.close()
    return rows


@app.get("/actions")
def list_actions(limit: int = 100) -> List[Dict[str, Any]]:
    # No execution queue yet; surface empty list for frontend compatibility.
    return []


@app.get("/raw")
def list_raw(limit: int = 100) -> List[Dict[str, Any]]:
    conn = connect_db(DB_PATH)
    rows = get_recent_raw(conn, limit=limit)
    conn.close()
    return rows


@app.get("/settings")
def get_settings() -> Dict[str, Any]:
    return load_settings()


@app.post("/settings")
def update_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    # Persist non-secret config; secrets should stay in env or upstream vault.
    allowed_keys = set(DEFAULT_SETTINGS.keys())
    filtered = {k: v for k, v in payload.items() if k in allowed_keys}
    if not filtered:
        raise HTTPException(status_code=400, detail="No valid settings supplied")
    return save_settings(filtered)


@app.get("/stats")
def stats() -> Dict[str, Any]:
    conn = connect_db(DB_PATH)
    data = get_event_statistics(conn)
    conn.close()
    return data


@app.get("/orders")
def orders(limit: int = 100) -> List[Dict[str, Any]]:
    return list_recent_orders(limit=limit)
