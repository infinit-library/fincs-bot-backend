from typing import Any, Dict, List, Optional

import os
import time
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import threading

from src import runtime_config
from src.config.setting import SaxoSettings
from .executor import list_recent_orders, run_execution_cycle
from src.auth.saxo_oauth import SaxoOAuthClient, Token
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

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"



def _load_env_file() -> None:
    if not ENV_PATH.exists():
        return
    for raw in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        os.environ[key] = val


def _update_env_vars(updates: Dict[str, str]) -> None:
    existing_lines = []
    if ENV_PATH.exists():
        existing_lines = ENV_PATH.read_text(encoding="utf-8").splitlines()

    keys = set(updates.keys())
    new_lines = []
    seen = set()

    for line in existing_lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            new_lines.append(line)
            continue
        key, _ = stripped.split("=", 1)
        if key in updates:
            new_lines.append(f"{key}={updates[key]}")
            seen.add(key)
        else:
            new_lines.append(line)

    for key in keys - seen:
        new_lines.append(f"{key}={updates[key]}")

    ENV_PATH.write_text("\n".join(new_lines) + "\n", encoding="utf-8")

    for key, val in updates.items():
        os.environ[key] = str(val)


def _persist_saxo_tokens(token: Token) -> Dict[str, Any]:
    updates = {
        "SAXO_ACCESS_TOKEN": token.access_token,
    }
    if token.refresh_token:
        updates["SAXO_REFRESH_TOKEN"] = token.refresh_token
    updates["SAXO_TOKEN_EXPIRES_AT"] = str(int(token.expires_at))
    if getattr(token, "refresh_expires_at", None):
        updates["SAXO_REFRESH_TOKEN_EXPIRES_AT"] = str(int(token.refresh_expires_at))
    _update_env_vars(updates)
    return {"expires_at": token.expires_at, "refresh_expires_at": getattr(token, "refresh_expires_at", None)}


def _refresh_saxo_tokens_if_possible() -> Optional[Dict[str, Any]]:
    refresh_token = os.getenv("SAXO_REFRESH_TOKEN")
    if not refresh_token:
        return None
    settings = SaxoSettings.from_env()
    oauth = SaxoOAuthClient(settings)
    refresh_expires_raw = os.getenv("SAXO_REFRESH_TOKEN_EXPIRES_AT")
    refresh_expires_at = float(refresh_expires_raw) if refresh_expires_raw else 0.0
    oauth.token = Token(access_token="", refresh_token=refresh_token, expires_at=0, refresh_expires_at=refresh_expires_at)
    oauth.refresh()
    if not oauth.token:
        return None
    return _persist_saxo_tokens(oauth.token)


def _saxo_token_state() -> Dict[str, Any]:
    _load_env_file()
    access = os.getenv("SAXO_ACCESS_TOKEN")
    refresh = os.getenv("SAXO_REFRESH_TOKEN")
    expires_raw = os.getenv("SAXO_TOKEN_EXPIRES_AT")
    refresh_expires_raw = os.getenv("SAXO_REFRESH_TOKEN_EXPIRES_AT")
    expires_at = float(expires_raw) if expires_raw else 0.0
    refresh_expires_at = float(refresh_expires_raw) if refresh_expires_raw else 0.0
    now = time.time()
    expired = bool(expires_at) and now >= (expires_at - 30)
    refresh_expired = bool(refresh_expires_at) and now >= (refresh_expires_at - 30)
    refresh_expires_in = None
    if refresh_expires_at:
        refresh_expires_in = max(0, int(refresh_expires_at - now))
    return {
        "has_access_token": bool(access),
        "has_refresh_token": bool(refresh),
        "expires_at": int(expires_at) if expires_at else None,
        "expired": expired,
        "refresh_expires_at": int(refresh_expires_at) if refresh_expires_at else None,
        "refresh_expired": refresh_expired,
        "refresh_expires_in_seconds": refresh_expires_in,
    }


def _ensure_saxo_tokens() -> Optional[str]:
    state = _saxo_token_state()
    if not state["has_access_token"] and not state["has_refresh_token"]:
        return "Saxo???????????URL???????????"
    if state.get("refresh_expired"):
        return "???????????????????????????????"
    if state["expired"] and not state["has_refresh_token"]:
        return "Saxo???????????????????????????"
    if (not state["has_access_token"] or state["expired"]) and state["has_refresh_token"]:
        try:
            _refresh_saxo_tokens_if_possible()
        except Exception as exc:
            return f"Saxo??????????????: {exc}"
    return None



_token_refresh_thread: Optional[threading.Thread] = None


def _start_token_refresher_if_needed() -> None:
    global _token_refresh_thread
    if _token_refresh_thread is not None and _token_refresh_thread.is_alive():
        return

    interval = int(os.getenv("SAXO_REFRESH_LOOP_INTERVAL", "60"))
    threshold = int(os.getenv("SAXO_AUTO_REFRESH_THRESHOLD", "120"))

    def _loop() -> None:
        while True:
            try:
                state = _saxo_token_state()
                expires_at = state.get("expires_at") or 0
                if expires_at and time.time() >= (float(expires_at) - threshold):
                    _refresh_saxo_tokens_if_possible()
            except Exception as exc:
                os.environ["SAXO_REFRESH_FAILED"] = "true"
                os.environ["SAXO_REFRESH_FAILED_REASON"] = str(exc)
            time.sleep(interval)

    _token_refresh_thread = threading.Thread(target=_loop, daemon=True)
    _token_refresh_thread.start()

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
    settings = runtime_config.load_settings()
    if not settings.get("running", False):
        settings["running"] = True
        runtime_config.save_settings(settings)
    _start_scheduler_if_needed()
    _start_token_refresher_if_needed()


@app.get("/status")
def status() -> Dict[str, Any]:
    conn = connect_db(DB_PATH)
    latest = get_latest_snapshot(conn)
    latest_signal: Optional[Dict[str, Any]] = get_latest_trading_event(conn)

    settings = runtime_config.load_settings()

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
    global _last_error
    token_error = _ensure_saxo_tokens()
    if token_error:
        _last_error = token_error
        raise HTTPException(status_code=401, detail=token_error)
    try:
        _refresh_saxo_tokens_if_possible()
    except Exception as exc:
        _last_error = str(exc)
    settings = runtime_config.load_settings()
    settings["running"] = True
    runtime_config.save_settings(settings)
    # kick scheduler thread if not running
    global _bg_thread
    if _bg_thread is None or not _bg_thread.is_alive():
        _stop_event.clear()
        _bg_thread = threading.Thread(target=run_scheduler, args=(_stop_event,), daemon=True)
        _bg_thread.start()
    return {"status": "running"}


@app.post("/bot/stop")
def bot_stop() -> Dict[str, str]:
    global _last_error
    settings = runtime_config.load_settings()
    settings["running"] = False
    runtime_config.save_settings(settings)
    _stop_event.set()
    _last_error = None
    return {"status": "stopped"}


@app.post("/bot/run-once")
def bot_run_once() -> Dict[str, Any]:
    global _last_error
    token_error = _ensure_saxo_tokens()
    if token_error:
        _last_error = token_error
        raise HTTPException(status_code=401, detail=token_error)
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
    return runtime_config.load_settings()


@app.post("/settings")
def update_settings(payload: Dict[str, Any]) -> Dict[str, Any]:
    # Persist non-secret config; secrets should stay in env or upstream vault.
    allowed_keys = set(runtime_config.DEFAULT_SETTINGS.keys())
    filtered = {k: v for k, v in payload.items() if k in allowed_keys}
    if not filtered:
        raise HTTPException(status_code=400, detail="No valid settings supplied")
    if "max_lot_cap" in filtered:
        try:
            max_lot_cap = float(filtered["max_lot_cap"])
        except Exception:
            raise HTTPException(status_code=400, detail="max_lot_cap must be a number")
        if max_lot_cap <= 0 or max_lot_cap > 1.0:
            raise HTTPException(status_code=400, detail="max_lot_cap must be > 0 and <= 1")
        filtered["max_lot_cap"] = max_lot_cap
    return runtime_config.save_settings(filtered)




@app.get("/saxo/auth-url")
def saxo_auth_url() -> Dict[str, Any]:
    settings = SaxoSettings.from_env()
    oauth = SaxoOAuthClient(settings)
    return {"url": oauth.authorization_url(), "redirect_uri": settings.redirect_uri, "environment": settings.environment}


@app.post("/saxo/auth-exchange")
def saxo_auth_exchange(payload: Dict[str, Any]) -> Dict[str, Any]:
    code = (payload or {}).get("code")
    if not code:
        raise HTTPException(status_code=400, detail="Missing code")
    settings = SaxoSettings.from_env()
    oauth = SaxoOAuthClient(settings)
    oauth.authenticate(str(code))
    if not oauth.token:
        raise HTTPException(status_code=500, detail="Token exchange failed")
    meta = _persist_saxo_tokens(oauth.token)
    return {"status": "ok", **meta}


@app.post("/saxo/refresh")
def saxo_refresh() -> Dict[str, Any]:
    meta = _refresh_saxo_tokens_if_possible()
    if not meta:
        raise HTTPException(status_code=400, detail="No refresh token available")
    return {"status": "ok", **meta}



@app.get("/saxo/health")
def saxo_health() -> Dict[str, Any]:
    details: Dict[str, Any] = {
        "env": os.getenv("SAXO_ENV", "sim"),
        "has_access_token": bool(os.getenv("SAXO_ACCESS_TOKEN")),
        "has_refresh_token": bool(os.getenv("SAXO_REFRESH_TOKEN")),
        "account_key": os.getenv("SAXO_ACCOUNT_KEY") or None,
        "client_key": os.getenv("SAXO_CLIENT_KEY") or None,
        "expires_at": None,
        "expired": False,
        "refresh_expires_at": None,
        "refresh_expired": False,
        "refresh_expires_in_seconds": None,
    }
    try:
        state = _saxo_token_state()
        details["expires_at"] = state["expires_at"]
        details["expired"] = state["expired"]
        details["refresh_expires_at"] = state.get("refresh_expires_at")
        details["refresh_expired"] = state.get("refresh_expired")
        details["refresh_expires_in_seconds"] = state.get("refresh_expires_in_seconds")
        settings = SaxoSettings.from_env()
        oauth = SaxoOAuthClient(settings)
        token = os.getenv("SAXO_ACCESS_TOKEN")
        refresh = os.getenv("SAXO_REFRESH_TOKEN")
        expires_at = os.getenv("SAXO_TOKEN_EXPIRES_AT")
        refresh_expires_at = os.getenv("SAXO_REFRESH_TOKEN_EXPIRES_AT")
        if token:
            oauth.token = Token(access_token=token, refresh_token=refresh, expires_at=float(expires_at or 0), refresh_expires_at=float(refresh_expires_at or 0))
        elif refresh:
            oauth.token = Token(access_token="", refresh_token=refresh, expires_at=0)
            oauth.refresh()
            if oauth.token:
                _persist_saxo_tokens(oauth.token)
        broker = None
        if oauth.token:
            from src.brokers.saxo import SaxoBroker

            broker = SaxoBroker(oauth)
            balance = broker.get_balance()
            equity = broker.get_equity()
            details["balance_ok"] = True
            details["equity"] = equity
            details["balance"] = balance
        details["ok"] = bool(oauth.token)
    except Exception as exc:
        details["ok"] = False
        details["error"] = str(exc)
    return details

@app.get("/stats")
def stats() -> Dict[str, Any]:
    conn = connect_db(DB_PATH)
    data = get_event_statistics(conn)
    conn.close()
    return data


@app.get("/orders")
def orders(limit: int = 100) -> List[Dict[str, Any]]:
    return list_recent_orders(limit=limit)
