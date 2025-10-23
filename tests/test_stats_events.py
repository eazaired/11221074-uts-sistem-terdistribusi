import pytest, asyncio

@pytest.mark.asyncio
async def test_stats_and_events_consistency(client):
    evs = [{
      "topic": "logs.consistency",
      "event_id": f"c-{i}",
      "timestamp": "2025-10-22T13:00:00Z",
      "source": "tester",
      "payload": {"n": i}
    } for i in range(5)]

    r = await client.post("/publish", json={"events": evs})
    assert r.status_code == 200

    # beri waktu consumer memproses
    await asyncio.sleep(0.8)

    res = await client.get("/events", params={"topic":"logs.consistency"})
    assert res.status_code == 200
    data = res.json()

    assert data["topic"] == "logs.consistency"
    assert len(data["events"]) >= 5
