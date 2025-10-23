# tests/conftest.py
import pytest
import httpx
from asgi_lifespan import LifespanManager
from src.main import app

@pytest.fixture
async def client():
    # Jalankan startup/shutdown FastAPI
    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)  # TANPA lifespan=...
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac
