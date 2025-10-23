import time, requests, uuid, datetime, random # type: ignore

URL = "http://aggregator:8080/publish"
topics = ["logs.app1.info", "logs.app1.error", "logs.app2.warn"]

def make_event():
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return {
      "topic": random.choice(topics),
      "event_id": f"pub-{uuid.uuid4()}",
      "timestamp": now,
      "source": "compose-publisher",
      "payload": {"value": random.randint(1, 1000)}
    }

print("Publisher started. Sending logs every second...")

while True:
    batch = [make_event() for _ in range(5)]
    try:
        res = requests.post(URL, json={"events": batch}, timeout=3)
        print(f"[{datetime.datetime.now().isoformat()}] Sent {len(batch)} events -> {res.status_code}")
    except Exception as e:
        print("Failed to send:", e)
    time.sleep(1)
