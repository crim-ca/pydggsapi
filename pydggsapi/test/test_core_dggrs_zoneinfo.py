from pydggsapi.schemas.ogc_dggs.dggrs_zones_info import ZoneInfoResponse
from fastapi.testclient import TestClient
import pytest
from importlib import reload
import pydggsapi.api
import os
from pprint import pprint
from dggrid4py import DGGRIDv7
import tempfile


def test_core_dggs_zoneinfo_empty_config():
    os.environ['dggs_api_config'] = './empty.json'
    app = reload(pydggsapi.api).app
    client = TestClient(app)
    print("Testing with dggrs zoneinfo (no dggrs defined)")
    response = client.get('/dggs-api/v1-pre/dggs/IGEO7/zones/123343054095')
    pprint(response.text)
    assert "No dggrs definition is found" in response.text
    assert response.status_code == 500

    print("Testing with collections dggrs zoneinfo (no dggrs defined)")
    response = client.get('/dggs-api/v1-pre/collections/suitability_hytruck/dggs/IGEO7/zones/123343054095')
    pprint(response.text)
    assert "No dggrs definition is found" in response.text
    assert response.status_code == 500


def test_core_dggs_zoneinfo():
    os.environ['dggs_api_config'] = './dggs_api_config.json'
    app = reload(pydggsapi.api).app
    client = TestClient(app)
    print("Fail test case with non existing dggrs id")
    response = client.get('/dggs-api/v1-pre/dggs/non_exist/zones/00000000')
    pprint(response.json())
    assert "not supported" in response.text
    assert response.status_code == 500

    os.environ['dggs_api_config'] = './dggs_api_config.json'
    app = reload(pydggsapi.api).app
    client = TestClient(app)
    print("Success test case with dggs zone info (IGEO7 00000000)")
    response = client.get('/dggs-api/v1-pre/dggs/IGEO7/zones/00000000')
    pprint(response.json())
    assert ZoneInfoResponse(**response.json())
    assert response.status_code == 200

    print("Fail test case with not exist collection dggs zone info (collection not found)")
    response = client.get('/dggs-api/v1-pre/collections/hytruck/dggs/IGEO7/zones/00000001')
    pprint(response.text)
    assert "hytruck not found" in response.text
    assert response.status_code == 500

    print("Fail test case with collections (non-existing dggrs id)")
    response = client.get('/dggs-api/v1-pre/collections/suitability_hytruck/dggs/Non_existing/zones/0023451345')
    pprint(response.text)
    assert "not supported" in response.text
    assert response.status_code == 500

    print("Success test case with collections on zones info (suitability_hytruck, IGEO7, 00000024)")
    response = client.get('/dggs-api/v1-pre/collections/suitablilty_hytruck/dggs/IGEO7/zones/00000024')
    pprint(response.json())
    assert ZoneInfoResponse(**response.json())
    assert response.status_code == 200
