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
cellids = ['841134dffffffff', '841136bffffffff', '841f65bffffffff', '8411345ffffffff', '8411369ffffffff']
# cellids = ['00010220', '0001022011', '0001022012']
zone_level = [5, 6, 7, 8, 9]

aoi = shapely.Polygon(aoi)


def test_data_retrieval_VH3():
    os.environ['dggs_api_config'] = './dggs_api_config.json'
    app = reload(pydggsapi.api).app
    client = TestClient(app)

    print(f"Success test case with data-retrieval query (VH3, {cellids[0]}, relative_depth=2)")
    response = client.get(f'/dggs-api/v1-pre/dggs/VH3/zones/{cellids[0]}/data', params={'depth': 2})
    pprint(response.json())
    data = ZonesDataDggsJsonResponse(**response.json())
    assert response.status_code == 200
