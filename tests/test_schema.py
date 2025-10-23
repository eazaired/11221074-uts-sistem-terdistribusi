import pytest

@pytest.mark.asyncio
async def test_schema_validation(client):

    bad = {
        "events": {
            "topic": "  bad.space  ",
            "event_id": "",
            "timestamp": "invalid",
            "source": "",
            "payload": {}
        }
    }
    r = await client.post("/publish", json=bad)
    assert r.status_code == 422  
