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

#
cellids = { 'ivea7h': [864691128455137448],
            'rhealpix': [3458764641595818679]
          }

non_exists = { 'ivea7h': [1044835113549955079],
               'rhealpix': [4035226506295773230]
             }

app = Application(appGlobals=globals())
pydggal_setup(app)

ivea7h = IVEA7H()
rhealpix = rHEALPix()

validation_hexagons= {'ivea7h': [] , 'rhealpix': []}
validation_centroids= {'ivea7h': [] , 'rhealpix': []}

for zone_id in cellids['ivea7h']:
    polygon = generateZoneGeometry(ivea7h, zone_id, None, False)
    head = polygon.coordinates[0][0]
    polygon.coordinates[0].append(head)
    centroid = generateZoneGeometry(ivea7h, zone_id, None, True)
    validation_hexagons['ivea7h'].append(shapely.from_geojson(json.dumps(polygon.__dict__)))
    validation_centroids['ivea7h'].append(shapely.from_geojson(json.dumps(centroid.__dict__)))

for zone_id in cellids['rhealpix']:
    polygon = generateZoneGeometry(rhealpix, zone_id, None, False)
    head = polygon.coordinates[0][0]
    polygon.coordinates[0].append(head)
    centroid = generateZoneGeometry(rhealpix, zone_id, None, True)
    validation_hexagons['rhealpix'].append(shapely.from_geojson(json.dumps(polygon.__dict__)))
    validation_centroids['rhealpix'].append(shapely.from_geojson(json.dumps(centroid.__dict__)))



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
        zoneinfo = ZoneInfoResponse(**response.json())
        head = zoneinfo.geometry.coordinates[0][0]
        zoneinfo.geometry.coordinates[0].append(head)
        centroid = shapely.from_geojson(json.dumps(zoneinfo.centroid.__dict__))
        hexagon = shapely.from_geojson(json.dumps(zoneinfo.geometry.__dict__))
        assert shapely.equals(hexagon, validation_hexagons[grid][0])
        assert shapely.equals(centroid, validation_centroids[grid][0])
        assert response.status_code == 200
        non_exists_zone_id = mygrid.getZoneTextID(non_exists[grid][0])
        print(f"Fail test case with collections on non-exist zones info (dggal_{grid}_hytruck, {grid}, {non_exists_zone_id})")
        response = client.get(f'/dggs-api/v1-pre/collections/dggal_{grid}_hytruck/dggs/{grid}/zones/{non_exists_zone_id}')
        assert response.status_code == 204
