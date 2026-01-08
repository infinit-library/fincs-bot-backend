import os

from dotenv import load_dotenv
import subprocess
import sys
import time

from src import runtime_config


def _run_module(module: str) -> int:
    return subprocess.call([sys.executable, "-m", module])


def main() -> None:
    load_dotenv(override=True)
    settings = runtime_config.load_settings()
    poll_interval = max(10, int(settings.get("poll_interval", 15)))

    if os.getenv("BOT_ENABLED", "") != "true":
        print("BOT_ENABLED is not true; watcher exiting.")
        return

    print(f"Watcher started. Poll interval: {poll_interval}s")
    while True:
        if os.getenv("BOT_ENABLED", "") != "true":
            print("BOT_ENABLED is not true; watcher exiting.")
            return

        print("Running scraper...")
        _run_module("src.login_fincs")

        print("Running executor...")
        _run_module("src.executor")

        time.sleep(poll_interval)


if __name__ == "__main__":
    main()
