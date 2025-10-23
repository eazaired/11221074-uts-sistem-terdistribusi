import os
import random
import asyncio
import pytest

@pytest.mark.asyncio
async def test_stress_small(client):
    total = int(os.getenv("TOTAL", "5000"))
    dup_ratio = 0.20
    uniques = int(total * (1 - dup_ratio)) 

    # 1) Base set yang unik
    base_ids = [f"u-{i}" for i in range(uniques)]
    events = [{
        "topic": "logs.stress",
        "event_id": eid,
        "timestamp": "2025-10-22T14:00:00Z",
        "source": "stress",
        "payload": {"kind": "unique", "i": i}
    } for i, eid in enumerate(base_ids)]

    # 2) Tambahkan duplikat dari base set (benar-benar duplikat)
    dups_needed = total - uniques
    for i in range(dups_needed):
        eid = random.choice(base_ids)
        events.append({
            "topic": "logs.stress",
            "event_id": eid,
            "timestamp": "2025-10-22T14:00:00Z",
            "source": "stress",
            "payload": {"kind": "duplicate", "i": uniques + i}
        })

    # Shuffle agar campur unik & duplikat
    random.shuffle(events)

    # 3) Kirim per-batch
    batch = 200
    for i in range(0, total, batch):
        r = await client.post("/publish", json={"events": events[i:i + batch]})
        assert r.status_code == 200

    # 4) Polling sampai consumer selesai memproses (maks 15 detik)
    #    (Windows + SQLite async kadang butuh jeda lebih lama)
    d = {}
    target_unique = uniques
    target_received = total
    for _ in range(300):  # 300 * 0.2s = 60 detik
        s = await client.get("/stats")
        d = s.json()
        if d.get("unique_processed", 0) >= target_unique and d.get("received", 0) >= target_received:
            break
        await asyncio.sleep(0.2)

    # 5) Verifikasi
    assert d["received"] >= target_received
    assert d["unique_processed"] >= target_unique
    # duplikat minimal mendekati (total - uniques). Toleransi kecil diperbolehkan.
    assert d["duplicate_dropped"] >= int(dup_ratio * total) - 5
