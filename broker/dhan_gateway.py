"""Dhan API broker implementation."""
import logging

from broker.base_gateway import BaseBrokerGateway

logger = logging.getLogger(__name__)


class DhanGateway(BaseBrokerGateway):
    """
    Broker gateway for Dhan.
    Library: dhanhq
    Docs: https://dhanhq.co/docs/v2/
    """

    def __init__(self, config: dict, instrument_manager=None):
        self.config = config
        self.instrument_manager = instrument_manager
        broker_cfg = config["broker"]["dhan"]
        self.client_id = broker_cfg.get("client_id", "")
        self.access_token = broker_cfg.get("access_token", "")
        self.dhan = None

    def connect(self) -> bool:
        try:
            from dhanhq import DhanHQ
            self.dhan = DhanHQ(
                client_id=self.client_id,
                access_token=self.access_token
            )
            # Validate by fetching fund limits
            result = self.dhan.get_fund_limits()
            if result.get("status") == "success":
                logger.info("Dhan connection established successfully")
                return True
            else:
                logger.error(f"Dhan connection failed: {result}")
                return False
        except Exception as e:
            logger.error(f"Dhan connection error: {e}")
            return False

    def place_order(self, symbol: str, side: str, quantity: int, order_type: str,
                    price: float = None, trigger_price: float = None,
                    product: str = "intraday") -> str:
        from dhanhq import DhanHQ
        security_id = self.instrument_manager.get_security_id(symbol) if self.instrument_manager else symbol

        order_type_map = {
            "MARKET": self.dhan.MARKET,
            "LIMIT": self.dhan.LIMIT,
            "SL": self.dhan.SL,
            "SL-M": self.dhan.SLM,
        }
        response = self.dhan.place_order(
            security_id=security_id,
            exchange_segment=self.dhan.NSE,
            transaction_type=self.dhan.BUY if side == "BUY" else self.dhan.SELL,
            quantity=quantity,
            order_type=order_type_map.get(order_type, self.dhan.MARKET),
            product_type=self.dhan.INTRA if product == "intraday" else self.dhan.CNC,
            price=price or 0,
            trigger_price=trigger_price or 0,
        )
        order_id = response["data"]["orderId"]
        logger.info(f"Order placed: {side} {quantity} {symbol} | OrderID: {order_id}")
        return order_id

    def cancel_order(self, order_id: str) -> bool:
        try:
            response = self.dhan.cancel_order(order_id)
            return response.get("status") == "success"
        except Exception as e:
            logger.error(f"Cancel order failed: {e}")
            return False

    def get_order_status(self, order_id: str) -> dict:
        response = self.dhan.get_order_by_id(order_id)
        status_map = {
            "TRADED": "FILLED",
            "PENDING": "PENDING",
            "REJECTED": "REJECTED",
            "CANCELLED": "CANCELLED",
            "TRANSIT": "PENDING",
        }
        data = response.get("data", {})
        return {
            "status": status_map.get(data.get("orderStatus", ""), "PENDING"),
            "fill_price": data.get("tradedPrice"),
        }

    def get_positions(self) -> list:
        response = self.dhan.get_positions()
        return response.get("data", [])

    def get_margins(self) -> dict:
        funds = self.dhan.get_fund_limits()
        data = funds.get("data", {})
        # Note: Dhan has a typo in their API — "availabelBalance"
        return {
            "available_cash": float(data.get("availabelBalance", data.get("availableBalance", 0))),
            "used_margin": float(data.get("utilizedAmount", 0)),
        }

    def get_ltp(self, symbol: str) -> float:
        security_id = self.instrument_manager.get_security_id(symbol) if self.instrument_manager else symbol
        response = self.dhan.get_market_quote(
            security_id=security_id,
            exchange_segment=self.dhan.NSE
        )
        return float(response["data"]["LTP"])

    def disconnect(self) -> None:
        self.dhan = None
        logger.info("Dhan connection closed")
