import websocket
import json
import redis
from database import SessionLocal
from models import Stock

API_KEY = "YOUR_KEY"
r = redis.Redis(host="localhost", port=6379, decode_responses=True)

def on_message(ws, message):
    data = json.loads(message)
    if data.get('type') == 'trade':
        for trade in data['data']:
            ticker = trade['s']
            price = trade['p']
            r.set(ticker, price)
            # Save to DB
            db = SessionLocal()
            stock = Stock(ticker=ticker, price=price)
            db.add(stock)
            db.commit()
            db.close()
    print(message)

def on_open(ws):
    ws.send(json.dumps({
        "type": "subscribe",
        "symbol": "AAPL"
    }))

ws = websocket.WebSocketApp(
    f"wss://ws.finnhub.io?token={API_KEY}",
    on_message=on_message
)

ws.on_open = on_open
ws.run_forever()