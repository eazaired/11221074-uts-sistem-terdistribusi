import asyncio
import pytest

@pytest.mark.asyncio
async def test_dedup_single(client):
    # event yang sama dikirim 3x
    ev = {
        "topic": "logs.test",
        "event_id": "id-1",
        "timestamp": "2025-10-22T12:00:00Z",
        "source": "tester",
        "payload": {"ok": True},
    }

    for _ in range(3):
        r = await client.post("/publish", json={"events": ev})
        assert r.status_code == 200

    # beri waktu consumer memproses queue
    await asyncio.sleep(0.6)

    # cek statistik
    s = await client.get("/stats")
    data = s.json()
    assert data["received"] >= 3
    assert data["unique_processed"] >= 1
    assert data["duplicate_dropped"] >= 2

    # opsional: cek /events untuk memastikan hanya 1 entri tersimpan
    res = await client.get("/events", params={"topic": "logs.test"})
    assert res.status_code == 200
    events = res.json()["events"]
    assert len(events) == 1
    assert events[0]["event_id"] == "id-1"
