from pydggsapi.schemas.ogc_dggs.dggrs_zones_info import ZoneInfoResponse
from pydggsapi.dependencies.dggrs_providers.dggal_dggrs_provider import generateZoneGeometry

from fastapi.testclient import TestClient
from dggal import *
import pytest
from importlib import reload
import os
from pprint import pprint
import shapely
import json

cellids = { 'ivea7h': [1261007895663811584, 1261007895670345984, 1297036692689212672],
            'rhealpix': [4035225790109976576, 4035225887820482560,4035225955466217472]
          }

non_exists = { 'ivea7h': [1297036692691480832],
               'rhealpix': [4135225955466217472]
             }

app = Application(appGlobals=globals())
pydggal_setup(app)

ivea7h = IVEA7H()
rhealpix = rHEALPix()

validation_hexagons= {'ivea7h': [] , 'rhealpix': []}
validation_centroids= {'ivea7h': [] , 'rhealpix': []}

for zone_id in cellids['ivea7h']:
    validation_hexagons['ivea7h'].append(generateZoneGeometry(ivea7h, zone_id, None, False))
    validation_centroids['ivea7h'].append(generateZoneGeometry(ivea7h, zone_id, None, True))

for zone_id in cellids['rhealpix']:
    validation_hexagons['rhealpix'].append(generateZoneGeometry(rhealpix, zone_id, None, False))
    validation_centroids['rhealpix'].append(generateZoneGeometry(rhealpix, zone_id, None, True))



def test_core_dggs_zoneinfo():
    os.environ['dggs_api_config'] = './dggs_api_config_testing.json'
    import pydggsapi.api
    app = reload(pydggsapi.api).app
    client = TestClient(app)


    for grid in list(cellids.keys()):
        mygrid = ivea7h if (grid == 'ivea7h') else rhealpix
        zone_id = mygrid.getZoneTextID(cellids[grid][0])
        print(f"Success test case with dggs zone info ( {zone_id})")
        response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones/{zone_id}')
        pprint(response.json())
        zoneinfo = ZoneInfoResponse(**response.json())
        centroid = shapely.from_geojson(json.dumps(zoneinfo.centroid.__dict__))
        hexagon = shapely.from_geojson(json.dumps(zoneinfo.geometry.__dict__))
        assert shapely.equals(hexagon, validation_hexagons[grid].loc[cellids[0]]['geometry'])
        assert shapely.equals(centroid, validation_centroids[grid].loc[cellids[0]]['geometry'])
        assert response.status_code == 200
        zone_id = mygrid.getZoneTextID(cellids[grid][2])
        print(f"Success test case with collections on zones info (suitability_hytruck, {grid}, {zone_id})")
        response = client.get(f'/dggs-api/v1-pre/collections/suitability_hytruck/dggs/{grid}/zones/{zone_id}')
        pprint(response.json())
        zoneinfo = ZoneInfoResponse(**response.json())
        centroid = shapely.from_geojson(json.dumps(zoneinfo.centroid.__dict__))
        hexagon = shapely.from_geojson(json.dumps(zoneinfo.geometry.__dict__))
        assert shapely.equals(hexagon, validation_hexagons[grid].loc[cellids[2]]['geometry'])
        assert shapely.equals(centroid, validation_centroids[grid].loc[cellids[2]]['geometry'])
        assert response.status_code == 200
        non_exists_zone_id = mygrid.getZoneTextID(non_exists[grid][0])
        print(f"Fail test case with collections on non-exist zones info (suitability_hytruck, {grid}, {non_exists_zone_id})")
        response = client.get(f'/dggs-api/v1-pre/collections/suitability_hytruck/dggs/{grid}/zones/{non_exists_zone_id}')
        assert response.status_code == 204
