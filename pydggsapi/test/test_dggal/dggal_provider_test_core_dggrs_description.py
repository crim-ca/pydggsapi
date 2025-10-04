from pydggsapi.schemas.ogc_dggs.dggrs_list import DggrsListResponse
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescription
from fastapi.testclient import TestClient
import pytest
from importlib import reload
import os
from pprint import pprint

support_grids =['ivea7h', 'rhealpix']

def test_core_dggs_description_empty_config():
    os.environ['dggs_api_config'] = './empty.json'
    try:
        import pydggsapi.api
        app = reload(pydggsapi.api).app
        client = TestClient(app)
    except Exception as e:
        print(f"Testing with dggrs definition (no dggrs defined): {e}")


def test_core_dggrs_description():
    os.environ['dggs_api_config'] = './dggs_api_config_testing.json'
    import pydggsapi.api
    app = reload(pydggsapi.api).app
    client = TestClient(app)
    print("Fail test case with non existing dggrs id")
    response = client.get('/dggs-api/v1-pre/dggs/Not_exisits')
    pprint(response.json())
    assert "not supported" in response.text
    assert response.status_code == 400

    os.environ['dggs_api_config'] = './dggs_api_config_testing.json'
    app = reload(pydggsapi.api).app
    client = TestClient(app)
    for grid in support_grids:
        print(f"Success test case with dggs description {grid})")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}')
        pprint(response.json())
        assert DggrsDescription(**response.json())
        assert response.status_code == 200
        print(f"Success test case with collections dggs-description (suitability_hytruck, {grid})")
        response = client.get(f'/dggs-api/v1-pre/collections/suitability_hytruck_{grid}/dggs/{grid}')
        pprint(response.json())
        assert DggrsDescription(**response.json())
        assert response.status_code == 200

