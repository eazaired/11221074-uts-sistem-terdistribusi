import aiosqlite
import json

CREATE_DEDUP = """
CREATE TABLE IF NOT EXISTS dedup (
  topic TEXT NOT NULL,
  event_id TEXT NOT NULL,
  PRIMARY KEY (topic, event_id)
);
"""

CREATE_EVENTS = """
CREATE TABLE IF NOT EXISTS events (
  topic TEXT NOT NULL,
  event_id TEXT NOT NULL,
  timestamp TEXT NOT NULL,
  source TEXT NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY (topic, event_id)
);
"""

CREATE_STATS = """
CREATE TABLE IF NOT EXISTS stats (
  key TEXT PRIMARY KEY,
  val INTEGER NOT NULL
);
"""

class Storage:
    def __init__(self, db_path: str = "data.db"):
        self.db_path = db_path
        self.db: aiosqlite.Connection | None = None

    async def init(self):
        self.db = await aiosqlite.connect(self.db_path)
        await self.db.execute("PRAGMA journal_mode=WAL;")
        await self.db.execute("PRAGMA synchronous=NORMAL;")
        await self.db.execute("PRAGMA foreign_keys=ON;")
        await self.db.execute(CREATE_DEDUP)
        await self.db.execute(CREATE_EVENTS)
        await self.db.execute(CREATE_STATS)
        await self.db.execute(
            "INSERT OR IGNORE INTO stats(key,val) VALUES "
            "('received',0),('unique_processed',0),('duplicate_dropped',0)"
        )
        await self.db.commit()

    async def close(self):
        if self.db is not None:
            await self.db.close()
            self.db = None

    async def try_insert_dedup(self, topic: str, event_id: str) -> bool:
        """True jika baru; False jika duplikat."""
        assert self.db is not None, "Storage not initialized"
        try:
            await self.db.execute(
                "INSERT INTO dedup(topic,event_id) VALUES(?,?)",
                (topic, event_id),
            )
            await self.db.execute(
                "UPDATE stats SET val = val + 1 WHERE key='unique_processed'"
            )
            await self.db.commit()
            return True
        except aiosqlite.IntegrityError:
            await self.db.execute(
                "UPDATE stats SET val = val + 1 WHERE key='duplicate_dropped'"
            )
            await self.db.commit()
            return False

    async def record_received(self, n: int):
        assert self.db is not None
        await self.db.execute(
            "UPDATE stats SET val = val + ? WHERE key='received'", (n,)
        )
        await self.db.commit()

    async def store_event(
        self, topic: str, event_id: str, timestamp: str, source: str, payload: dict
    ):
        assert self.db is not None
        await self.db.execute(
            "INSERT OR IGNORE INTO events(topic,event_id,timestamp,source,payload)"
            " VALUES(?,?,?,?,?)",
            (topic, event_id, timestamp, source, json.dumps(payload)),
        )
        await self.db.commit()

    async def list_topics(self) -> list[str]:
        assert self.db is not None
        cur = await self.db.execute(
            "SELECT DISTINCT topic FROM events ORDER BY topic"
        )
        rows = await cur.fetchall()
        return [r[0] for r in rows]

    async def get_events_by_topic(self, topic: str) -> list[dict]:
        assert self.db is not None
        cur = await self.db.execute(
            "SELECT topic,event_id,timestamp,source,payload "
            "FROM events WHERE topic=? ORDER BY timestamp",
            (topic,),
        )
        rows = await cur.fetchall()
        out = []
        for t, eid, ts, src, pl in rows:
            out.append(
                {
                    "topic": t,
                    "event_id": eid,
                    "timestamp": ts,
                    "source": src,
                    "payload": json.loads(pl),
                }
            )
        return out

    async def get_stats(self) -> dict:
        assert self.db is not None
        cur = await self.db.execute("SELECT key, val FROM stats")
        rows = await cur.fetchall()
        return {k: v for k, v in rows}
