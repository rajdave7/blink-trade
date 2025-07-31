import asyncio
from fastapi import FastAPI, WebSocket
from aiokafka import AIOKafkaConsumer
import json

app = FastAPI()
TOPIC = "top_of_book"
BOOTSTRAP = "kafka:9092"

# In-memory store of latest snapshot per symbol (optional)
latest = {}

@app.on_event("startup")
async def startup_event():
    # Start Kafka consumer
    app.consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id="streamer-group",
        auto_offset_reset="latest"
    )
    await app.consumer.start()
    # Launch background task to read Kafka
    asyncio.create_task(consume_loop())

@app.on_event("shutdown")
async def shutdown_event():
    await app.consumer.stop()

async def consume_loop():
    async for msg in app.consumer:
        data = json.loads(msg.value.decode())
        latest[data["symbol"]] = data
        # Broadcast to all connected websockets
        for ws in app.websocket_connections:
            await ws.send_text(json.dumps(data))

# Store active WebSocket connections
app.websocket_connections = set()

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    app.websocket_connections.add(ws)
    try:
        # Optionally send current state on connect
        for data in latest.values():
            await ws.send_text(json.dumps(data))
        while True:
            await ws.receive_text()  # keep connection open
    finally:
        app.websocket_connections.remove(ws)
