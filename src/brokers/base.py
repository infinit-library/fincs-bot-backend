from abc import ABC, abstractmethod
from typing import Any


class BaseBroker(ABC):

    @abstractmethod
    def get_accounts(self) -> Any:
        pass

    @abstractmethod
    def get_balance(self) -> Any:
        pass

    @abstractmethod
    def get_price(self, symbol: str) -> Any:
        pass
