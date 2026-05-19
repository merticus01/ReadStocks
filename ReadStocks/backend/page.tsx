"use client"
import { useState, useEffect, useRef } from "react"

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000"

interface Quote {
  ticker: string
  current_price: number
  change: number
  percent_change: number
  high: number
  low: number
  open: number
  previous_close: number
  cached?: boolean
}

export default function Dashboard() {
  const [ticker, setTicker] = useState("AAPL")
  const [input, setInput] = useState("AAPL")
  const [quote, setQuote] = useState<Quote | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [livePrice, setLivePrice] = useState<number | null>(null)
  const wsRef = useRef<WebSocket | null>(null)

  const fetchQuote = async (t: string) => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch(`${API_BASE}/stock/${t}`)
      if (!res.ok) {
        const body = await res.json()
        throw new Error(body.detail ?? "Failed to fetch quote.")
      }
      setQuote(await res.json())
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Unknown error")
    } finally {
      setLoading(false)
    }
  }

  // REST poll on ticker change
  useEffect(() => {
    fetchQuote(ticker)
  }, [ticker])

  // WebSocket for live updates
  useEffect(() => {
    wsRef.current?.close()
    const ws = new WebSocket(`${API_BASE.replace("http", "ws")}/ws/${ticker}`)
    ws.onmessage = (e) => {
      const data = JSON.parse(e.data)
      if (data.price) setLivePrice(data.price)
    }
    wsRef.current = ws
    return () => ws.close()
  }, [ticker])

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    if (input.trim()) setTicker(input.trim().toUpperCase())
  }

  const pctColor = quote
    ? quote.percent_change >= 0
      ? "text-green-600"
      : "text-red-600"
    : ""

  return (
    <main className="min-h-screen bg-gray-50 p-6">
      <h1 className="text-3xl font-bold mb-6">📈 Stock Dashboard</h1>

      {/* Search */}
      <form onSubmit={handleSearch} className="flex gap-2 mb-6">
        <input
          className="border rounded px-3 py-2 text-lg uppercase"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ticker (e.g. TSLA)"
        />
        <button
          type="submit"
          className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
        >
          Search
        </button>
      </form>

      {loading && <p className="text-gray-500">Loading…</p>}
      {error && <p className="text-red-600">Error: {error}</p>}

      {quote && !loading && (
        <div className="bg-white shadow rounded-xl p-6 max-w-md">
          <div className="flex items-center justify-between mb-2">
            <h2 className="text-2xl font-bold">{quote.ticker}</h2>
            {quote.cached && (
              <span className="text-xs bg-gray-100 text-gray-500 px-2 py-0.5 rounded">
                cached
              </span>
            )}
          </div>

          {/* Live price from WebSocket */}
          <p className="text-4xl font-semibold">
            ${(livePrice ?? quote.current_price).toFixed(2)}
            {livePrice && <span className="text-sm text-blue-500 ml-2">● live</span>}
          </p>

          <p className={`text-lg ${pctColor}`}>
            {quote.change >= 0 ? "▲" : "▼"} {Math.abs(quote.change).toFixed(2)} (
            {quote.percent_change.toFixed(2)}%)
          </p>

          <div className="mt-4 grid grid-cols-2 gap-2 text-sm text-gray-600">
            <span>Open: ${quote.open.toFixed(2)}</span>
            <span>Prev Close: ${quote.previous_close.toFixed(2)}</span>
            <span>High: ${quote.high.toFixed(2)}</span>
            <span>Low: ${quote.low.toFixed(2)}</span>
          </div>

          <button
            onClick={() => fetchQuote(ticker)}
            className="mt-4 text-blue-600 text-sm hover:underline"
          >
            Refresh
          </button>
        </div>
      )}
    </main>
  )
}
