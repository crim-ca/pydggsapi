from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataDggsJsonResponse, ZonesDataGeoJson
from fastapi.testclient import TestClient
import pytest
from importlib import reload
import os
from pprint import pprint
from dggrid4py import DGGRIDv7
import tempfile
import shapely
import json
import geopandas as gpd
import xarray as xr
import zarr

working = tempfile.mkdtemp()

cellids = { 'ivea7h': [1261007895663811584, 1261007895670345984, 1297036692689212672],
            'rhealpix': [4035225790109976576, 4035225887820482560,4035225955466217472]
          }

non_exists = { 'ivea7h': [1297036692691480832],
               'rhealpix': [4135225955466217472]
             }

zone_level = [5, 6, 7, 8, 9]



def test_data_retrieval():
    os.environ['dggs_api_config'] = './dggs_api_config_testing.json'
    import pydggsapi.api
    app = reload(pydggsapi.api).app
    client = TestClient(app)


    for grid in list(cellids.keys()):
        print(f"Fail test case withdata-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=4) over refinement")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'depth': 4})
        pprint(response.json())
        assert "not supported" in response.text
        assert response.status_code == 400

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]})")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data')
        pprint(response.json())
        data = ZonesDataDggsJsonResponse(**response.json())
        p1 = list(data.properties.keys())[0]
        assert len(data.values[p1]) > 0
        value = data.values[p1][0]
        assert len(value.data) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, return = geojson)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', headers={'accept': 'application/geo+json'})
        pprint(response.json())
        data = ZonesDataGeoJson(**response.json())
        assert len(data.features) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, return = geojson, geometry='zone-centroid')")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'geometry': 'zone-centroid'},
                              headers={'accept': 'application/geo+json'})
        pprint(response.json())
        data = ZonesDataGeoJson(**response.json())
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=2)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'depth': 2})
        pprint(response.json())
        data = ZonesDataDggsJsonResponse(**response.json())
        p1 = list(data.properties.keys())[0]
        assert len(data.values[p1]) > 0
        assert (8 in data.depths)
        value = data.values[p1][0]
        assert len(value.data) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=1-2)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'depth': '1-2'})
        pprint(response.json())
        data = ZonesDataDggsJsonResponse(**response.json())
        p1 = list(data.properties.keys())[0]
        assert len(data.values[p1]) > 0
        assert (7 in data.depths) and (8 in data.depths)
        value = data.values[p1][0]
        assert len(value.data) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=0-2)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/{grid}/{cellids[grid][0]}/data', params={'depth': '0-2'})
        pprint(response.json())
        data = ZonesDataDggsJsonResponse(**response.json())
        p1 = list(data.properties.keys())[0]
        assert len(data.values[p1]) > 0
        assert (7 in data.depths) and (8 in data.depths) and (6 in data.depths)
        value = data.values[p1][0]
        assert len(value.data) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=0-2, geometry='zone-centroid', return=geojson)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'depth': '0-2', 'geometry': 'zone-centroid'},
                              headers={'accept': 'application/geo+json'})
        pprint(response.json())
        data = ZonesDataGeoJson(**response.json())
        assert len(data.features) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=0-2, geometry='zone-centroid', return=zarr+zip)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'depth': '0-2', 'geometry': 'zone-centroid'},
                              headers={'accept': 'application/zarr+zip'})
        assert response.status_code == 200
        with open("data_zarr.zip", "wb") as f:
            f.write(response.content)
        z = zarr.open('data_zarr.zip')
        print(z.tree())

        print(f"Empty test case with data-retrieval query ({grid}, {non_exists[grid][0]}, relative_depth=0-2, geometry='zone-centroid', return=geojson)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{non_exists[grid][0]}/data', params={'depth': '0-2', 'geometry': 'zone-centroid'},
                              headers={'accept': 'application/geo+json'})
        assert response.status_code == 204
