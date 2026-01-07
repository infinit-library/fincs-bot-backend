import threading
import time
from datetime import datetime, timezone
from typing import Optional

from src import runtime_config
from .executor import run_execution_cycle
from .storage import connect_db, get_latest_snapshot, DB_PATH

# Lightweight heartbeat for the API
_last_attempt: Optional[str] = None
_last_success: Optional[str] = None
_last_error: Optional[str] = None


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _scrape_once_safe() -> Optional[dict]:
    try:
        from .login_fincs import scrape_once

        return scrape_once()
    except Exception as exc:
        return {"error": str(exc)}


def get_scrape_health() -> dict:
    """Expose scheduler heartbeat for status endpoint."""
    return {
        "last_attempt": _last_attempt,
        "last_success": _last_success,
        "last_error": _last_error,
    }


def run_scheduler(stop_event: threading.Event) -> None:
    """
    Background loop: when settings.running is True, run scraper once then execute trades.
    """
    global _last_attempt, _last_success, _last_error

    while not stop_event.is_set():
        settings = runtime_config.load_settings()
        if settings.get("running", False):
            import os

            os.environ["HEADLESS"] = "true" if settings.get("headless_scrape", True) else "false"
            _last_attempt = _utcnow()

            try:
                conn = connect_db(DB_PATH)
                before = get_latest_snapshot(conn)
                res = _scrape_once_safe()
                after = get_latest_snapshot(conn)
                conn.close()

                if isinstance(res, dict) and res.get("error"):
                    _last_error = res["error"]
                elif after and (not before or after.get("id") != before.get("id")):
                    _last_success = after.get("scraped_at") or _utcnow()
                    _last_error = None
                else:
                    # Scrape ran but no new data inserted
                    _last_error = None
            except Exception as exc:
                _last_error = str(exc)

            try:
                run_execution_cycle()
            except Exception as exc:
                # keep loop alive; surface last error
                _last_error = _last_error or str(exc)
        poll = max(5, int(settings.get("poll_interval", runtime_config.DEFAULT_SETTINGS["poll_interval"])))
        stop_event.wait(poll)
