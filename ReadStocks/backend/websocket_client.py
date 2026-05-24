import os
import websocket
import json
import redis
from backend.database import SessionLocal
from backend.models import Stock

API_KEY = os.getenv("FINNHUB_API_KEY", "YOUR_KEY")
r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=int(os.getenv("REDIS_PORT", 6379)), decode_responses=True)


def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "trade":
        for trade in data.get("data", []):
            ticker = trade.get("s")
            price = trade.get("p")
            if ticker and price is not None:
                r.set(ticker, price)
                # Save to DB
                db = SessionLocal()
                try:
                    stock = Stock(ticker=ticker, price=price)
                    db.add(stock)
                    db.commit()
                except Exception:
                    db.rollback()
                finally:
                    db.close()
    print(message)


def on_open(ws):
    ws.send(json.dumps({"type": "subscribe", "symbol": "AAPL"}))


def main():
    ws = websocket.WebSocketApp(
        f"wss://ws.finnhub.io?token={API_KEY}",
        on_message=on_message,
    )
    ws.on_open = on_open
    ws.run_forever()


if __name__ == "__main__":
    main()