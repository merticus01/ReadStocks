"""
Finnhub WebSocket Manager - Real-time stock price streaming.
Maintains persistent connection to Finnhub WebSocket and broadcasts updates.
"""
import asyncio
import json
import logging
import os
from typing import Set, Optional, Dict, Any
from contextlib import asynccontextmanager

import websocket
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from backend.database import SessionLocal
from backend.models import Stock

load_dotenv()

logger = logging.getLogger(__name__)

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
FINNHUB_WS_URL = "wss://ws.finnhub.io"

# Track which tickers have active subscribers
subscribed_tickers: Set[str] = set()
# Store latest prices for quick access
price_cache: Dict[str, Any] = {}
# Callbacks for WebSocket updates
update_callbacks: list[callable] = []


class FinnhubWSManager:
    def __init__(self):
        self.ws = None
        self.running = False
        self.subscribed: Set[str] = set()
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 2  # Start with 2 seconds

    def on_message(self, ws, message: str):
        """Handle incoming WebSocket messages from Finnhub."""
        try:
            data = json.loads(message)

            if data.get("type") == "trade":
                self._handle_trades(data)
            elif data.get("type") == "ping":
                logger.debug("Received ping from Finnhub")
            else:
                logger.debug(f"Received message: {data}")

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse WebSocket message: {e}")
        except Exception as e:
            logger.error(f"Error in on_message: {e}")

    def _handle_trades(self, data: dict):
        """Process trade data and persist to database/cache."""
        trades = data.get("data", [])
        db = None

        try:
            db = SessionLocal()

            for trade in trades:
                ticker = trade.get("s")
                price = trade.get("p")
                timestamp = trade.get("t")

                if not ticker or price is None:
                    continue

                # Update cache
                price_cache[ticker] = {
                    "price": price,
                    "timestamp": timestamp,
                    "symbol": ticker,
                }

                # Save to database
                try:
                    stock = Stock(ticker=ticker, price=price)
                    db.add(stock)
                    db.commit()
                    logger.debug(f"Saved {ticker} @ ${price} to DB")
                except Exception as e:
                    logger.warning(f"Failed to save {ticker} to DB: {e}")
                    db.rollback()

                # Call registered callbacks
                for callback in update_callbacks:
                    try:
                        asyncio.create_task(callback({
                            "ticker": ticker,
                            "price": price,
                            "timestamp": timestamp,
                            "type": "trade",
                        }))
                    except Exception as e:
                        logger.error(f"Callback error: {e}")

        except Exception as e:
            logger.error(f"Error handling trades: {e}")
        finally:
            if db:
                db.close()

    def on_error(self, ws, error: Exception):
        """Handle WebSocket errors."""
        logger.error(f"WebSocket error: {error}")
        self.running = False

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket closure."""
        logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
        self.running = False
        self.ws = None

    def on_open(self, ws):
        """Handle WebSocket connection open."""
        logger.info("Connected to Finnhub WebSocket")
        self.running = True
        self.reconnect_attempts = 0
        self.reconnect_delay = 2
        
        # Re-subscribe to all tracked tickers
        for ticker in self.subscribed:
            self._send_subscribe(ticker)

    def _send_subscribe(self, ticker: str):
        """Send subscription message for a ticker."""
        if not self.ws or not self.running:
            logger.warning(f"Cannot subscribe to {ticker}: WS not connected")
            return

        try:
            msg = json.dumps({
                "type": "subscribe",
                "symbol": ticker.upper(),
            })
            self.ws.send(msg)
            logger.info(f"Subscribed to {ticker}")
        except Exception as e:
            logger.error(f"Failed to subscribe to {ticker}: {e}")

    def _send_unsubscribe(self, ticker: str):
        """Send unsubscription message for a ticker."""
        if not self.ws or not self.running:
            return

        try:
            msg = json.dumps({
                "type": "unsubscribe",
                "symbol": ticker.upper(),
            })
            self.ws.send(msg)
            logger.info(f"Unsubscribed from {ticker}")
        except Exception as e:
            logger.error(f"Failed to unsubscribe from {ticker}: {e}")

    def subscribe(self, ticker: str):
        """Subscribe to a ticker's real-time updates."""
        ticker = ticker.upper()
        if ticker not in self.subscribed:
            self.subscribed.add(ticker)
            self._send_subscribe(ticker)

    def unsubscribe(self, ticker: str):
        """Unsubscribe from a ticker's updates."""
        ticker = ticker.upper()
        if ticker in self.subscribed:
            self.subscribed.remove(ticker)
            self._send_unsubscribe(ticker)

    def connect(self):
        """Establish connection to Finnhub WebSocket."""
        if not FINNHUB_API_KEY:
            logger.error("FINNHUB_API_KEY not set - cannot connect to WebSocket")
            return False

        try:
            url = f"{FINNHUB_WS_URL}?token={FINNHUB_API_KEY}"
            self.ws = websocket.WebSocketApp(
                url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open,
            )
            logger.info("Finnhub WebSocket connection initiated")
            return True
        except Exception as e:
            logger.error(f"Failed to create WebSocket: {e}")
            return False

    def run_forever(self):
        """Run WebSocket connection with automatic reconnection."""
        while True:
            try:
                if not self.ws:
                    if not self.connect():
                        logger.error("Failed to connect, retrying...")
                        asyncio.run(asyncio.sleep(self.reconnect_delay))
                        continue

                logger.info("Starting WebSocket loop...")
                self.ws.run_forever(
                    ping_interval=30,
                    ping_payload="ping",
                )
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
                self.running = False
                self.ws = None

            # Exponential backoff for reconnection
            if self.reconnect_attempts < self.max_reconnect_attempts:
                self.reconnect_attempts += 1
                delay = min(self.reconnect_delay * (2 ** self.reconnect_attempts), 60)
                logger.info(f"Reconnecting in {delay} seconds... (attempt {self.reconnect_attempts})")
                asyncio.run(asyncio.sleep(delay))
            else:
                logger.error("Max reconnection attempts reached, stopping...")
                break


# Global manager instance
_manager: Optional[FinnhubWSManager] = None


def get_manager() -> FinnhubWSManager:
    """Get or create the WebSocket manager instance."""
    global _manager
    if _manager is None:
        _manager = FinnhubWSManager()
    return _manager


async def register_update_callback(callback: callable):
    """Register a callback to be called on price updates."""
    update_callbacks.append(callback)
    logger.info(f"Registered callback: {callback.__name__}")


async def unregister_update_callback(callback: callable):
    """Unregister an update callback."""
    if callback in update_callbacks:
        update_callbacks.remove(callback)
        logger.info(f"Unregistered callback: {callback.__name__}")


@asynccontextmanager
async def run_finnhub_ws_background():
    """Context manager to run WebSocket in background task."""
    manager = get_manager()
    
    # Run WebSocket in a separate thread
    loop = asyncio.get_event_loop()
    task = loop.run_in_executor(None, manager.run_forever)
    
    try:
        yield manager
    finally:
        # Graceful shutdown
        if manager.ws:
            manager.ws.close()
        task.cancel()
        logger.info("Finnhub WebSocket manager shut down")
