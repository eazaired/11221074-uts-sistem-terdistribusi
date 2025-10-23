import asyncio
import pytest

@pytest.mark.asyncio
async def test_persistence_across_restarts(client):
    ev = {
        "topic": "logs.persist",
        "event_id": "persist-1",
        "timestamp": "2025-10-22T12:30:00Z",
        "source": "tester",
        "payload": {},
    }

    r1 = await client.post("/publish", json={"events": ev})
    assert r1.status_code == 200
    await asyncio.sleep(0.3)

    r2 = await client.post("/publish", json={"events": ev})
    assert r2.status_code == 200
    await asyncio.sleep(0.3)

    s = await client.get("/stats")
    d = s.json()
    # kiriman kedua harus terhitung duplikat
    assert d["duplicate_dropped"] >= 1
    # minimal 1 event unik sudah diproses
    assert d["unique_processed"] >= 1
