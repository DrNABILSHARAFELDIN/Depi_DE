import json
import asyncio
import websockets
from confluent_kafka import Producer

# Configure Kafka Producer Client
conf = {'bootstrap.servers': "localhost:9092"}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"📡 Event pushed to Topic: {msg.topic()} | Partition: {msg.partition()}")

async def stream_binance():
    # Public Binance Live trade WebSocket endpoint
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    
    # 'async for' ensures that if the websocket disconnects, it reconnects automatically
    async for websocket in websockets.connect(url, ping_interval=30, ping_timeout=30):
        print("🚀 Successfully connected to Binance Live Stream API...")
        try:
            while True:
                # Receive real-world trade data stream
                raw_data = await websocket.recv()
                trade_event = json.loads(raw_data)
                
                # Queue message string into Kafka Topic (Non-blocking)
                producer.produce(
                    topic="crypto_trades", 
                    value=json.dumps(trade_event), 
                    callback=delivery_report
                )
                
                # ✅ FIX 1: Triggers callbacks asynchronously without blocking the event loop
                producer.poll(0)
                
                # Yield control back to the asyncio loop for a tiny fraction of a second
                await asyncio.sleep(0.01)
                
        except websockets.exceptions.ConnectionClosed:
            print("⚠️ Connection dropped by network/server. Reconnecting automatically...")
            continue

if __name__ == "__main__":
    try:
        asyncio.run(stream_binance())
    except KeyboardInterrupt:
        print("\nStopping Ingestion Engine.")
        # ✅ FIX 2: Only flush remaining messages once when safely shutting down
        print("Flushing final message buffers to Kafka...")
        producer.flush(timeout=3)