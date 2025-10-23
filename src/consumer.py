import asyncio
from .storage import Storage

class Consumer:
    """Worker sederhana yang membaca event dari asyncio.Queue dan menyimpan
    hanya event unik (idempotent) berdasarkan (topic,event_id)."""

    def __init__(self, storage: Storage, queue: "asyncio.Queue[dict]"):
        self.storage = storage
        self.queue = queue
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._running = False
        if self._task:
            # kirim sentinel agar loop berhenti rapi
            await self.queue.put(None)  # type: ignore
            await self._task

    async def _run(self):
        while self._running:
            item = await self.queue.get()
            if item is None:  # sentinel
                self.queue.task_done()
                break

            topic = item["topic"]
            event_id = item["event_id"]
            is_new = await self.storage.try_insert_dedup(topic, event_id)
            if is_new:
                await self.storage.store_event(
                    topic=topic,
                    event_id=event_id,
                    timestamp=item["timestamp"],
                    source=item["source"],
                    payload=item["payload"],
                )
            # selesai proses item
            self.queue.task_done()
