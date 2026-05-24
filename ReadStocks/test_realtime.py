#!/usr/bin/env python3
"""
Quick Test Script for Real-Time Stock Updates
Run this to verify the WebSocket connection is working
"""
import asyncio
import json
import websockets
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("API_URL", "ws://localhost:8000")

async def test_websocket():
    """Connect to the WebSocket and display real-time updates."""
    ticker = "AAPL"
    uri = f"{API_URL}/ws/{ticker}"
    
    print(f"Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri) as websocket:
            print(f"✓ Connected! Listening for real-time updates for {ticker}...")
            print()
            
            # Listen for updates
            update_count = 0
            while True:
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=30.0)
                    data = json.loads(message)
                    
                    update_count += 1
                    
                    if "error" in data:
                        print(f"❌ Error: {data['error']}")
                    else:
                        print(f"[Update #{update_count}] {ticker} @ ${data.get('price', 'N/A')}")
                        if data.get('timestamp'):
                            print(f"  Timestamp: {data['timestamp']}")
                        print()
                        
                except asyncio.TimeoutError:
                    print("⚠ No updates for 30 seconds (market may be closed)")
                    break
                except json.JSONDecodeError as e:
                    print(f"❌ Failed to parse message: {e}")
                    
    except ConnectionRefusedError:
        print("❌ Connection refused - is the API running on localhost:8000?")
    except Exception as e:
        print(f"❌ Connection error: {e}")

if __name__ == "__main__":
    print("ReadStocks Real-Time Update Tester")
    print("=" * 40)
    
    try:
        asyncio.run(test_websocket())
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
