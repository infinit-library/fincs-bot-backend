import os
import requests
from src.config.setting import SaxoSettings


settings = SaxoSettings.from_env()
token = os.getenv("SAXO_ACCESS_TOKEN")
if not token:
    raise SystemExit("Missing SAXO_ACCESS_TOKEN")

url = f"{settings.base_url}/port/v1/accounts/me"
for attempt in range(3):
    try:
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
        break
    except requests.exceptions.ReadTimeout:
        if attempt >= 2:
            raise
        continue
print("status:", resp.status_code)
print(resp.text)
