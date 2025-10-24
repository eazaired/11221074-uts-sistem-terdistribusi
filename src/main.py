# src/main.py
import asyncio
from typing import Any
from contextlib import asynccontextmanager
import os

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from .models import PublishRequest, EventList, Stats, Event
from .storage import Storage
from .consumer import Consumer
from .utils import Uptime

def create_app(db_path: str | None = None) -> FastAPI:
    # Tentukan DB path dari argumen atau ENV (default ke data/data.db)
    db_path = db_path or os.getenv("DB_PATH", "data/data.db")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    app = FastAPI(title="UTS Pub-Sub Log Aggregator", version="1.0.0")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.storage = Storage(db_path)
        app.state.queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue() # type: ignore
        app.state.consumer = Consumer(app.state.storage, app.state.queue)
        app.state.uptime = Uptime()

        await app.state.storage.init()
        await app.state.consumer.start()
        try:
            yield
        finally:
            await app.state.consumer.stop()
            await app.state.storage.close()

    app.router.lifespan_context = lifespan 

    @app.post("/publish")
    async def publish(req: PublishRequest):
        events: list[Event]
        if isinstance(req.events, Event):
            events = [req.events]
        else:
            events = req.events

        await app.state.storage.record_received(len(events))
        for ev in events:
            await app.state.queue.put(ev.model_dump())
        return JSONResponse({"status": "queued", "count": len(events)})

    @app.get("/events", response_model=EventList)
    async def get_events(topic: str = Query(..., min_length=1)):
        events = await app.state.storage.get_events_by_topic(topic)
        return {"topic": topic, "events": events}

    @app.get("/stats", response_model=Stats)
    async def stats():
        s = await app.state.storage.get_stats()
        topics = await app.state.storage.list_topics()
        return {
            "received": s.get("received", 0),
            "unique_processed": s.get("unique_processed", 0),
            "duplicate_dropped": s.get("duplicate_dropped", 0),
            "topics": topics,
            "uptime_seconds": app.state.uptime.seconds,
        }

    return app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("src.main:app", host="0.0.0.0", port=8080, reload=True)
