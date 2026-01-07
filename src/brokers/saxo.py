import requests
from src.auth.saxo_oauth import SaxoOAuthClient
from src.config.settings import SaxoSettings



class SaxoBroker:
    def __init__(self, oauth: SaxoOAuthClient, settings: SaxoSettings):
        self.oauth = oauth
        self.base_url = settings.base_url

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.oauth.get_access_token()}",
            "Content-Type": "application/json",
        }

    def get_accounts(self) -> dict:
        url = f"{self.base_url}/port/v1/accounts/me"
        response = requests.get(url, headers=self._headers(), timeout=10)
        response.raise_for_status()
        return response.json()

    def get_balance(self) -> dict:
        url = f"{self.base_url}/port/v1/balances"
        response = requests.get(url, headers=self._headers(), timeout=10)
        response.raise_for_status()
        return response.json()

    def get_price(self, uic: int) -> dict:
        """
        Example: USDJPY UIC is provided by Saxo (client confirms)
        """
        url = f"{self.base_url}/trade/v1/prices"
        params = {
            "Uic": uic,
            "AssetType": "FxSpot",
        }
        response = requests.get(
            url, headers=self._headers(), params=params, timeout=10
        )
        response.raise_for_status()
        return response.json()
