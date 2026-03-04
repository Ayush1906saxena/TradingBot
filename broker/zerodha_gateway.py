"""Zerodha Kite Connect broker implementation."""
import logging

from broker.base_gateway import BaseBrokerGateway

logger = logging.getLogger(__name__)


class ZerodhaGateway(BaseBrokerGateway):
    """
    Broker gateway for Zerodha.
    Library: kiteconnect
    Docs: https://kite.trade/docs/connect/v3/
    """

    def __init__(self, config: dict, instrument_manager=None):
        self.config = config
        self.instrument_manager = instrument_manager
        broker_cfg = config["broker"]["zerodha"]
        self.api_key = broker_cfg.get("api_key", "")
        self.api_secret = broker_cfg.get("api_secret", "")
        self.totp_secret = broker_cfg.get("totp_secret", "")
        self.kite = None

    def connect(self) -> bool:
        try:
            from kiteconnect import KiteConnect
            self.kite = KiteConnect(api_key=self.api_key)

            if self.totp_secret:
                return self._auto_login()
            else:
                login_url = self.kite.login_url()
                logger.warning(
                    f"Manual login required. Open this URL:\n{login_url}\n"
                    "Then call set_access_token() with the request_token from the redirect URL."
                )
                return False
        except Exception as e:
            logger.error(f"Zerodha connection error: {e}")
            return False

    def _auto_login(self) -> bool:
        """Automated login using TOTP."""
        try:
            import pyotp
            import requests
            from kiteconnect import KiteConnect

            totp = pyotp.TOTP(self.totp_secret).now()
            session = requests.Session()

            # NOTE: This auto-login may break if Zerodha changes their login flow.
            # Fallback: user manually visits kite.login_url() and pastes request_token.
            logger.info("Zerodha TOTP auto-login not fully implemented. Manual login required.")
            return False
        except Exception as e:
            logger.error(f"Auto-login failed: {e}")
            return False

    def set_access_token(self, request_token: str) -> None:
        """Call this manually after getting request_token from login redirect."""
        data = self.kite.generate_session(request_token, api_secret=self.api_secret)
        self.kite.set_access_token(data["access_token"])
        logger.info("Zerodha access token set successfully")

    def place_order(self, symbol: str, side: str, quantity: int, order_type: str,
                    price: float = None, trigger_price: float = None,
                    product: str = "intraday") -> str:
        order_type_map = {
            "MARKET": self.kite.ORDER_TYPE_MARKET,
            "LIMIT": self.kite.ORDER_TYPE_LIMIT,
            "SL": self.kite.ORDER_TYPE_SL,
            "SL-M": self.kite.ORDER_TYPE_SLM,
        }
        order_id = self.kite.place_order(
            variety=self.kite.VARIETY_REGULAR,
            tradingsymbol=symbol,
            exchange=self.kite.EXCHANGE_NSE,
            transaction_type=self.kite.TRANSACTION_TYPE_BUY if side == "BUY"
                             else self.kite.TRANSACTION_TYPE_SELL,
            quantity=quantity,
            order_type=order_type_map.get(order_type, self.kite.ORDER_TYPE_MARKET),
            product=self.kite.PRODUCT_MIS if product == "intraday" else self.kite.PRODUCT_CNC,
            price=price,
            trigger_price=trigger_price,
            validity=self.kite.VALIDITY_DAY,
        )
        logger.info(f"Zerodha order: {side} {quantity} {symbol} | OrderID: {order_id}")
        return str(order_id)

    def cancel_order(self, order_id: str) -> bool:
        try:
            self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR, order_id=order_id)
            return True
        except Exception as e:
            logger.error(f"Cancel order failed: {e}")
            return False

    def get_order_status(self, order_id: str) -> dict:
        orders = self.kite.orders()
        for order in orders:
            if str(order["order_id"]) == str(order_id):
                status_map = {
                    "COMPLETE": "FILLED",
                    "OPEN": "PENDING",
                    "REJECTED": "REJECTED",
                    "CANCELLED": "CANCELLED",
                }
                return {
                    "status": status_map.get(order["status"], "PENDING"),
                    "fill_price": order.get("average_price"),
                }
        return {"status": "PENDING", "fill_price": None}

    def get_positions(self) -> list:
        return self.kite.positions().get("net", [])

    def get_margins(self) -> dict:
        margins = self.kite.margins(segment="equity")
        return {
            "available_cash": float(margins.get("available", {}).get("live_balance", 0)),
            "used_margin": float(margins.get("utilised", {}).get("debits", 0)),
        }

    def get_ltp(self, symbol: str) -> float:
        quote = self.kite.ltp(f"NSE:{symbol}")
        return float(quote[f"NSE:{symbol}"]["last_price"])

    def disconnect(self) -> None:
        self.kite = None
        logger.info("Zerodha connection closed")
