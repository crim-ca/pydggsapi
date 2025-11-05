from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataDggsJsonResponse, ZonesDataGeoJson
from fastapi.testclient import TestClient
import pytest
from dggal import *
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


app = Application(appGlobals=globals())
pydggal_setup(app)

ivea7h = IVEA7H()
rhealpix = rHEALPix()

# refinement level 7
cellids = {'ivea7h': [ivea7h.getZoneTextID(z) for z in [864691128455137439, 1044835113549955076, 864691128455137434]],
           'rhealpix': [rhealpix.getZoneTextID(z) for z in [4035225646228572205, 4035225647302314029, 4035226506295773228]]}
# refinement level 6
non_exists = {'ivea7h': [ivea7h.getZoneTextID(z) for z in [1044835113549955073]],
              'rhealpix': [rhealpix.getZoneTextID(z) for z in [3458764927211143866]]}

zone_level = [5, 6, 7, 8, 9]


def test_data_retrieval():
    os.environ['dggs_api_config'] = './dggs_api_config_testing.json'
    import pydggsapi.api
    app = reload(pydggsapi.api).app
    client = TestClient(app)

    for grid in list(cellids.keys()):
        print(f"Fail test case withdata-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=4) over refinement")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'zone-depth': 4})
        assert "over refinement" in response.text
        assert response.status_code == 400

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]})")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data')
        data = ZonesDataDggsJsonResponse(**response.json())
        p1 = list(data.properties.keys())[0]
        assert len(data.values[p1]) > 0
        value = data.values[p1][0]
        assert len(value.data) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, return = geojson)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', headers={'accept': 'application/geo+json'})
        data = ZonesDataGeoJson(**response.json())
        assert len(data.features) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, return = geojson, geometry='zone-centroid')")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'geometry': 'zone-centroid'},
                              headers={'accept': 'application/geo+json'})
        data = ZonesDataGeoJson(**response.json())
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=2)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'zone-depth': 2})
        data = ZonesDataDggsJsonResponse(**response.json())
        p1 = list(data.properties.keys())[0]
        assert len(data.values[p1]) > 0
        assert (8 in data.depths)
        value = data.values[p1][0]
        assert len(value.data) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=1-2)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'zone-depth': '1-2'})
        data = ZonesDataDggsJsonResponse(**response.json())
        p1 = list(data.properties.keys())[0]
        assert len(data.values[p1]) > 0
        assert (7 in data.depths) and (8 in data.depths)
        value = data.values[p1][0]
        assert len(value.data) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=0-2)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/{grid}/{cellids[grid][0]}/data', params={'zone-depth': '0-2'})
        data = ZonesDataDggsJsonResponse(**response.json())
        p1 = list(data.properties.keys())[0]
        assert len(data.values[p1]) > 0
        assert (7 in data.depths) and (8 in data.depths) and (6 in data.depths)
        value = data.values[p1][0]
        assert len(value.data) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=0-2, geometry='zone-centroid', return=geojson)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'zone-depth': '0-2', 'geometry': 'zone-centroid'},
                              headers={'accept': 'application/geo+json'})
        data = ZonesDataGeoJson(**response.json())
        assert len(data.features) > 0
        assert response.status_code == 200

        print(f"Success test case with data-retrieval query ({grid}, {cellids[grid][0]}, relative_depth=0-2, geometry='zone-centroid', return=zarr+zip)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{cellids[grid][0]}/data', params={'zone-depth': '0-2', 'geometry': 'zone-centroid'},
                              headers={'accept': 'application/zarr+zip'})
        assert response.status_code == 200
        with open("data_zarr.zip", "wb") as f:
            f.write(response.content)
        z = zarr.open('data_zarr.zip')
        print(z.tree())

        print(f"Empty test case with data-retrieval query ({grid}, {non_exists[grid][0]}, relative_depth=0-2, geometry='zone-centroid', return=geojson)")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{non_exists[grid][0]}/data', params={'zone-depth': '0-2', 'geometry': 'zone-centroid'},
                              headers={'accept': 'application/geo+json'})
        assert response.status_code == 204
