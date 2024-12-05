from fastapi.testclient import TestClient

from pydggsapi.api import app

client = TestClient(app)


def test_core_dggs_list():
    response = client.get('/collection/hytruck')
    assert response.status_code == 500

