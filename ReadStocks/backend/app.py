"""
ReadStocks API - FastAPI backend for stock data.
Supports Lovable/Bolt AI frontends via REST + WebSocket.
Real-time streaming via Finnhub WebSocket for live updates.
"""
import os
import json
import asyncio
import logging
import threading
from contextlib import asynccontextmanager
from typing import Optional

import redis
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from sqlalchemy.orm import Session

from backend.database import SessionLocal, engine, get_db
from backend.models import Base, Stock
from backend.finnhub_ws import get_manager, register_update_callback, unregister_update_callback

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
API_KEY = os.getenv("API_KEY", "")
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
    # Startup
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created.")
    
    # Initialize Finnhub WebSocket Manager
    manager = get_manager()
    ws_thread = threading.Thread(target=manager.run_forever, daemon=True)
    ws_thread.start()
    logger.info("Finnhub WebSocket manager started in background thread.")
    
    yield
    
    # Shutdown
    if manager.ws:
        manager.ws.close()
    logger.info("Finnhub WebSocket manager shut down.")


app = FastAPI(
    title="ReadStocks API",
    description="Real-time stock data API for trading apps built with Lovable/Bolt AI.",
    version="1.0.0",
    lifespan=lifespan,
)


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    openapi_schema.setdefault("components", {})
    openapi_schema["components"]["securitySchemes"] = {
        "ApiKeyHeader": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
        },
        "RapidAPIKeyHeader": {
            "type": "apiKey",
            "in": "header",
            "name": "X-RapidAPI-Key",
        },
    }
    openapi_schema["security"] = [
        {"ApiKeyHeader": []},
        {"RapidAPIKeyHeader": []},
    ]
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

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


def _get_request_api_key(request: Request) -> Optional[str]:
    return (
        request.headers.get("x-api-key")
        or request.headers.get("x-rapidapi-key")
        or request.headers.get("authorization", "").removeprefix("Bearer ").strip()
        or request.query_params.get("api_key")
    )


EXEMPT_PATHS = {"/health", "/docs", "/openapi.json", "/redoc"}


@app.middleware("http")
async def validate_api_key(request: Request, call_next):
    if not API_KEY or request.url.path in EXEMPT_PATHS:
        return await call_next(request)

    key = _get_request_api_key(request)
    if key != API_KEY:
        return JSONResponse({"detail": "Invalid or missing API key."}, status_code=401)

    return await call_next(request)


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
        self.active: dict[str, list[WebSocket]] = {}  # ticker -> [websockets]

    async def connect(self, ticker: str, ws: WebSocket):
        await ws.accept()
        if ticker not in self.active:
            self.active[ticker] = []
        self.active[ticker].append(ws)
        
        # Subscribe to ticker in Finnhub
        manager = get_manager()
        manager.subscribe(ticker)
        logger.info(f"WebSocket client connected for {ticker}. Active connections: {len(self.active[ticker])}")

    def disconnect(self, ticker: str, ws: WebSocket):
        if ticker in self.active and ws in self.active[ticker]:
            self.active[ticker].remove(ws)
            if not self.active[ticker]:
                # Unsubscribe if no more clients
                manager = get_manager()
                manager.unsubscribe(ticker)
                del self.active[ticker]
                logger.info(f"All clients disconnected from {ticker}, unsubscribing.")
            else:
                logger.info(f"WebSocket client disconnected from {ticker}. Active: {len(self.active[ticker])}")

    async def broadcast(self, ticker: str, data: dict):
        """Broadcast update to all clients watching this ticker."""
        if ticker not in self.active:
            return
        
        for ws in list(self.active[ticker]):
            try:
                await ws.send_json(data)
            except Exception as e:
                logger.warning(f"Failed to send to client: {e}")
                self.active[ticker].remove(ws)


manager_conn = ConnectionManager()


async def price_update_callback(update: dict):
    """Callback triggered when Finnhub sends a price update."""
    ticker = update.get("ticker")
    if ticker:
        await manager_conn.broadcast(ticker, update)


@app.websocket("/ws/{ticker}")
async def websocket_ticker(websocket: WebSocket, ticker: str):
    """
    WebSocket endpoint for real-time price updates.
    Receives live trade data from Finnhub WebSocket and streams to client.
    """
    if API_KEY:
        ws_key = (
            websocket.headers.get("x-api-key")
            or websocket.headers.get("x-rapidapi-key")
            or websocket.query_params.get("api_key")
        )
        if ws_key != API_KEY:
            await websocket.close(code=1008)
            return

    ticker = ticker.upper()
    await manager_conn.connect(ticker, websocket)
    
    # Register callback for this connection
    await register_update_callback(price_update_callback)
    
    try:
        # Keep connection alive and wait for client messages
        while True:
            # Receive messages (client might send ping/control messages)
            data = await websocket.receive_text()
            msg = json.loads(data)
            
            # Handle special messages
            if msg.get("action") == "subscribe":
                new_ticker = msg.get("ticker", "").upper()
                if new_ticker:
                    await manager_conn.connect(new_ticker, websocket)
                    logger.info(f"Client subscribed to additional ticker: {new_ticker}")
            elif msg.get("action") == "unsubscribe":
                unsub_ticker = msg.get("ticker", "").upper()
                if unsub_ticker:
                    manager_conn.disconnect(unsub_ticker, websocket)
                    logger.info(f"Client unsubscribed from ticker: {unsub_ticker}")
                    
    except WebSocketDisconnect:
        logger.info(f"WS client disconnected from {ticker}")
        manager_conn.disconnect(ticker, websocket)
        await unregister_update_callback(price_update_callback)
    except json.JSONDecodeError:
        logger.warning("Received invalid JSON from WebSocket client")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager_conn.disconnect(ticker, websocket)
