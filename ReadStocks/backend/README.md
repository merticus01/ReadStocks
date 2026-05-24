# ReadStocks API

A FastAPI backend + Next.js frontend for real-time stock data, built to integrate with Lovable and Bolt AI generated frontends.

## Quick Start

### 1. Get a free Finnhub API key
Sign up at [finnhub.io](https://finnhub.io) — the free tier covers all endpoints used here.

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env and set FINNHUB_API_KEY=your_key
# Set API_KEY to a shared application key for all clients
```

Add this to `backend/.env`:
```env
API_KEY=your_secret_key_here
```

RapidAPI users can send either header:
```http
X-API-Key: your_secret_key_here
X-RapidAPI-Key: your_secret_key_here
```

The API OpenAPI docs now declare these auth headers, so `/docs` will show the required security scheme for clients.

### 3. Run with Docker Compose (recommended)
```bash
docker compose up --build
```

API is now at `http://localhost:8000`  
Docs at `http://localhost:8000/docs`

### 4. Run locally (without Docker)
```bash
pip install -r requirements.txt
uvicorn backend.app:app --reload
```

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness probe |
| GET | `/stock/{ticker}` | Real-time quote |
| GET | `/stock/{ticker}/history` | Price history from DB |
| GET | `/stock/{ticker}/profile` | Company profile |
| GET | `/stock/{ticker}/news` | Recent news |
| GET | `/stocks/search?q=apple` | Search by keyword |
| WS | `/ws/{ticker}` | Live price stream (5s polling) |

## Using in Lovable / Bolt AI

Set the environment variable in your frontend project:
```
NEXT_PUBLIC_API_URL=http://localhost:8000
```

The API has **CORS open for all origins** by default (configurable via `ALLOWED_ORIGINS` in `.env`).

Example fetch call:
```js
const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/stock/AAPL`, {
  headers: {
    "X-API-Key": process.env.NEXT_PUBLIC_API_KEY,
  },
})
const { current_price, percent_change } = await res.json()
```

If you use RapidAPI, send the same key as `X-RapidAPI-Key`.

## Architecture

```
Frontend (Next.js / Lovable / Bolt)
        │  REST + WebSocket
        ▼
FastAPI (backend/app.py)
  ├── Redis (10s quote cache)
  ├── PostgreSQL (price history)
  └── Finnhub API (upstream data)
```
