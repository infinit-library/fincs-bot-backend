import abc
from typing import Dict, Any


class BaseBroker(abc.ABC):
    """Abstract broker interface for read-only operations."""

    @abc.abstractmethod
    def get_account_info(self) -> Dict[str, Any]:
        ...

    @abc.abstractmethod
    def get_balance(self) -> Dict[str, Any]:
        ...

    @abc.abstractmethod
    def get_positions(self) -> Dict[str, Any]:
        ...

    @abc.abstractmethod
    def get_price(self, symbol: str) -> Dict[str, Any]:
        ...
