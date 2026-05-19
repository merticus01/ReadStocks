import axios from "axios"
import { useState, useEffect } from "react"

export default function Dashboard() {
  const [stock, setStock] = useState(null)

  const fetchStock = async () => {
    const res = await axios.get("http://localhost:8000/stock/AAPL")
    setStock(res.data)
  }

  useEffect(() => {
    fetchStock()
  }, [])

  return (
    <div className="p-4">
      <h1 className="text-2xl font-bold">Stock Dashboard</h1>
      {stock && <p>AAPL: ${stock.c}</p>}
    </div>
  )
}