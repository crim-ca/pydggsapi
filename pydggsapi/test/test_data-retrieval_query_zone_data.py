from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataDggsJsonResponse, ZonesDataGeoJson
from fastapi.testclient import TestClient
import pytest
from importlib import reload
import pydggsapi.api
import os
from pprint import pprint
from dggrid4py import DGGRIDv7
import tempfile
import shapely
import json
import geopandas as gpd

working = tempfile.mkdtemp()
dggrid = DGGRIDv7(os.environ['DGGRID_PATH'], working_dir=working)

aoi = [[25.329803558251513, 58.634545591972696],
       [25.329803558251513, 57.99111013411327],
       [27.131561370751513, 57.99111013411327],
       [27.131561370751513, 58.634545591972696]]

aoi_3035 = [5204952.96287564, 3973761.18085118, 5324408.86305371, 4067507.93907037]
cellids = ['00010220', '0001022011', '0001022012']
zone_level = [5, 6, 7, 8, 9]

aoi = shapely.Polygon(aoi)


def test_data_retrieval_empty_config():
    os.environ['dggs_api_config'] = './empty.json'
    app = reload(pydggsapi.api).app
    client = TestClient(app)
    print("Testing with data-retrieval (no dggrs defined)")
    response = client.get(f'/dggs-api/v1-pre/dggs/IGEO7/zones/{cellids[0]}/data')
    pprint(response.text)
    assert "table not found" in response.text
    assert response.status_code == 500

    # print("Testing with collections dggrs zoneinfo (no dggrs defined)")
    # response = client.get('/dggs-api/v1-pre/collections/suitability_hytruck/dggs/IGEO7/zones/123343054095')
    # pprint(response.text)
    # assert "No dggrs definition is found" in response.text
    # assert response.status_code == 500


def test_data_retrieval():
    os.environ['dggs_api_config'] = './dggs_api_config.json'
    app = reload(pydggsapi.api).app
    client = TestClient(app)
    print("Fail test case with non existing dggrs id")
    response = client.get(f'/dggs-api/v1-pre/dggs/non_exist/zones/{cellids[0]}/data')
    pprint(response.json())
    assert "not supported" in response.text
    assert response.status_code == 500

    print(f"Fail test case withdata-retrieval query (IGEO7, {cellids[0]}, relative_depth=4) over refinement")
    response = client.get(f'/dggs-api/v1-pre/dggs/IGEO7/zones/{cellids[0]}/data', params={'depth': 4})
    pprint(response.json())
    assert "not supported" in response.text
    assert response.status_code == 500

    print(f"Success test case with data-retrieval query (IGEO7, {cellids[0]})")
    response = client.get(f'/dggs-api/v1-pre/dggs/IGEO7/zones/{cellids[0]}/data')
    pprint(response.json())
    data = ZonesDataDggsJsonResponse(**response.json())
    assert response.status_code == 200

    print(f"Success test case with data-retrieval query (IGEO7, {cellids[0]}, return = geojson)")
    response = client.get(f'/dggs-api/v1-pre/dggs/IGEO7/zones/{cellids[0]}/data', headers={'accept': 'application/geo+json'})
    pprint(response.json())
    data = ZonesDataGeoJson(**response.json())
    assert response.status_code == 200

    print(f"Success test case with data-retrieval query (IGEO7, {cellids[0]}, return = geojson, geometry='zone-centroid')")
    response = client.get(f'/dggs-api/v1-pre/dggs/IGEO7/zones/{cellids[0]}/data', params={'geometry': 'zone-centroid'},
                          headers={'accept': 'application/geo+json'})
    pprint(response.json())
    data = ZonesDataGeoJson(**response.json())
    assert response.status_code == 200

    print(f"Success test case with data-retrieval query (IGEO7, {cellids[0]}, relative_depth=2)")
    response = client.get(f'/dggs-api/v1-pre/dggs/IGEO7/zones/{cellids[0]}/data', params={'depth': 2})
    pprint(response.json())
    data = ZonesDataDggsJsonResponse(**response.json())
    assert response.status_code == 200

    print(f"Success test case with data-retrieval query (IGEO7, {cellids[0]}, relative_depth=1-2)")
    response = client.get(f'/dggs-api/v1-pre/dggs/IGEO7/zones/{cellids[0]}/data', params={'depth': '1-2'})
    pprint(response.json())
    data = ZonesDataDggsJsonResponse(**response.json())
    assert response.status_code == 200

    print(f"Success test case with data-retrieval query (IGEO7, {cellids[0]}, relative_depth=0-2)")
    response = client.get(f'/dggs-api/v1-pre/dggs/IGEO7/zones/{cellids[0]}/data', params={'depth': '0-2'})
    pprint(response.json())
    data = ZonesDataDggsJsonResponse(**response.json())
    assert response.status_code == 200

    print(f"Success test case with data-retrieval query (IGEO7, {cellids[0]}, relative_depth=0-2, geometry='zone-centroid', return=geojson)")
    response = client.get(f'/dggs-api/v1-pre/dggs/IGEO7/zones/{cellids[0]}/data', params={'depth': '0-2', 'geometry': 'zone-centroid'},
                          headers={'accept': 'application/geo+json'})
    pprint(response.json())
    data = ZonesDataGeoJson(**response.json())
    assert response.status_code == 200
