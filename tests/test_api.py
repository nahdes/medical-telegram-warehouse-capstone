from fastapi.testclient import TestClient
import pytest

from api.main import app

client = TestClient(app)


def test_root_health():
    resp = client.get('/')
    assert resp.status_code == 200
    data = resp.json()
    assert data['status'] == 'healthy'


def test_protected_endpoint_no_key():
    resp = client.get('/api/reports/top-products')
    assert resp.status_code == 403 or resp.status_code == 422


# override DB session to avoid actual database calls
from api.database import get_db_session

def fake_session():
    class Dummy:
        def execute(self, *args, **kwargs):
            return []
        def close(self):
            pass
    yield Dummy()


app.dependency_overrides[get_db_session] = fake_session


def test_top_products_with_key():
    resp = client.get('/api/reports/top-products', headers={'X-API-Key': 'changeme'})
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
