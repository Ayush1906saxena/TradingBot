"""Abstract broker interface."""
from abc import ABC, abstractmethod


class BaseBrokerGateway(ABC):

    @abstractmethod
    def connect(self) -> bool:
        pass

    @abstractmethod
    def place_order(self, symbol: str, side: str, quantity: int, order_type: str,
                    price: float = None, trigger_price: float = None,
                    product: str = "intraday") -> str:
        """Place order. Returns broker order ID."""
        pass

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        pass

    @abstractmethod
    def get_order_status(self, order_id: str) -> dict:
        """Returns { "status": "FILLED"|"PENDING"|"REJECTED"|"CANCELLED", "fill_price": float|None }"""
        pass

    @abstractmethod
    def get_positions(self) -> list:
        pass

    @abstractmethod
    def get_margins(self) -> dict:
        """Returns { "available_cash": float, "used_margin": float }"""
        pass

    @abstractmethod
    def get_ltp(self, symbol: str) -> float:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass
