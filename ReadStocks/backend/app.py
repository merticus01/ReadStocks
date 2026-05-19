"""
ReadStocks API - FastAPI backend for stock data.
Supports Lovable/Bolt AI frontends via REST + WebSocket.
"""
import os
import json
import logging
from contextlib import asynccontextmanager
from typing import Optional

import redis
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from database import SessionLocal, engine, get_db
from models import Base, Stock

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Redis client (optional - degrades gracefully if unavailable)
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    redis_client.ping()
    REDIS_AVAILABLE = True
    logger.info("Redis connected.")
except Exception:
    redis_client = None
    REDIS_AVAILABLE = False
    logger.warning("Redis not available; caching disabled.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created.")
    yield


app = FastAPI(
    title="ReadStocks API",
    description="Real-time stock data API for trading apps built with Lovable/Bolt AI.",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────────────────────
# Allow all origins for local dev / AI-generated frontends.
# Restrict ALLOWED_ORIGINS in production via env var.
allowed_origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cache_get(key: str) -> Optional[dict]:
    if REDIS_AVAILABLE:
        val = redis_client.get(key)
        if val:
            return json.loads(val)
    return None


def _cache_set(key: str, data: dict, ttl: int = 10):
    if REDIS_AVAILABLE:
        redis_client.setex(key, ttl, json.dumps(data))


def _finnhub_get(path: str, params: dict) -> dict:
    if not FINNHUB_API_KEY:
        raise HTTPException(status_code=503, detail="FINNHUB_API_KEY not configured.")
    params["token"] = FINNHUB_API_KEY
    resp = requests.get(f"https://finnhub.io/api/v1/{path}", params=params, timeout=5)
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Upstream API error.")
    return resp.json()


# ── REST endpoints ─────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Liveness probe — useful for Docker / cloud deployments."""
    return {"status": "ok", "redis": REDIS_AVAILABLE}


@app.get("/stock/{ticker}", summary="Get real-time quote")
def get_stock(ticker: str, db: Session = Depends(get_db)):
    """
    Returns the latest quote for a ticker symbol.
    Results are cached in Redis for 10 seconds.
    The price is also persisted to the database.
    """
    ticker = ticker.upper()
    cache_key = f"quote:{ticker}"

    cached = _cache_get(cache_key)
    if cached:
        cached["cached"] = True
        return cached

    data = _finnhub_get("quote", {"symbol": ticker})

    # Validate that we got a real quote (Finnhub returns zeros for unknown tickers)
    if data.get("c", 0) == 0:
        raise HTTPException(status_code=404, detail=f"No data found for ticker '{ticker}'.")

    result = {
        "ticker": ticker,
        "current_price": data["c"],
        "change": data["d"],
        "percent_change": data["dp"],
        "high": data["h"],
        "low": data["l"],
        "open": data["o"],
        "previous_close": data["pc"],
        "cached": False,
    }

    _cache_set(cache_key, result, ttl=10)

    # Persist to DB
    try:
        stock = Stock(ticker=ticker, price=data["c"])
        db.add(stock)
        db.commit()
    except Exception as e:
        logger.warning("DB write failed: %s", e)
        db.rollback()

    return result


@app.get("/stock/{ticker}/history", summary="Get recent price history from DB")
def get_stock_history(ticker: str, limit: int = 50, db: Session = Depends(get_db)):
    """Returns the last N recorded prices for a ticker (from local DB)."""
    ticker = ticker.upper()
    rows = (
        db.query(Stock)
        .filter(Stock.ticker == ticker)
        .order_by(Stock.timestamp.desc())
        .limit(limit)
        .all()
    )
    return [{"ticker": r.ticker, "price": r.price, "timestamp": r.timestamp} for r in rows]


@app.get("/stock/{ticker}/profile", summary="Get company profile")
def get_company_profile(ticker: str):
    """Returns company name, industry, logo, market cap, etc."""
    ticker = ticker.upper()
    cache_key = f"profile:{ticker}"
    cached = _cache_get(cache_key)
    if cached:
        return cached
    data = _finnhub_get("stock/profile2", {"symbol": ticker})
    if not data.get("name"):
        raise HTTPException(status_code=404, detail=f"No profile found for '{ticker}'.")
    _cache_set(cache_key, data, ttl=3600)
    return data


@app.get("/stock/{ticker}/news", summary="Get recent company news")
def get_stock_news(ticker: str, count: int = 10):
    """Returns recent news articles for a ticker."""
    ticker = ticker.upper()
    from datetime import date, timedelta
    today = date.today().isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    data = _finnhub_get(
        "company-news",
        {"symbol": ticker, "from": week_ago, "to": today},
    )
    return data[:count]


@app.get("/stocks/search", summary="Search for tickers by keyword")
def search_stocks(q: str):
    """Search for stocks by company name or ticker symbol."""
    data = _finnhub_get("search", {"q": q})
    return data.get("result", [])


# ── WebSocket for live price streaming ────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)

    async def broadcast(self, data: dict):
        for ws in list(self.active):
            try:
                await ws.send_json(data)
            except Exception:
                self.active.remove(ws)


manager = ConnectionManager()


@app.websocket("/ws/{ticker}")
async def websocket_ticker(websocket: WebSocket, ticker: str):
    """
    WebSocket endpoint. Connect to receive live price updates for a ticker.
    The server polls Finnhub every 5 seconds and pushes updates.
    """
    import asyncio
    ticker = ticker.upper()
    await websocket.accept()
    logger.info("WS client connected for %s", ticker)
    try:
        while True:
            try:
                data = _finnhub_get("quote", {"symbol": ticker})
                await websocket.send_json({
                    "ticker": ticker,
                    "price": data.get("c"),
                    "change": data.get("d"),
                    "percent_change": data.get("dp"),
                })
            except HTTPException as e:
                await websocket.send_json({"error": e.detail})
            except Exception as e:
                await websocket.send_json({"error": str(e)})
            await asyncio.sleep(5)
    except WebSocketDisconnect:
        logger.info("WS client disconnected for %s", ticker)
