import os
import threading
import time
from typing import Optional

from src.auth.saxo_oauth import SaxoOAuthClient


def start_token_refresher(oauth: SaxoOAuthClient) -> threading.Thread:
    """Background refresh loop to keep tokens alive without manual login."""
    interval = int(os.getenv("SAXO_REFRESH_LOOP_INTERVAL", "60"))
    threshold = int(os.getenv("SAXO_AUTO_REFRESH_THRESHOLD", "60"))

    def _loop() -> None:
        while True:
            try:
                # Trigger refresh if within threshold
                if oauth.access_token:
                    if time.time() >= (oauth.expires_at - threshold):
                        oauth.refresh_access_token()
                else:
                    # No token yet; sleep and wait
                    pass
            except Exception as exc:
                # Optional: flag failure for downstream logic
                os.environ["SAXO_REFRESH_FAILED"] = "true"
                os.environ["SAXO_REFRESH_FAILED_REASON"] = str(exc)
            time.sleep(interval)

    t = threading.Thread(target=_loop, daemon=True)
    t.start()
    return t
