from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesResponse, ZonesGeoJson
from pydggsapi.dependencies.dggrs_providers.dggal_dggrs_provider import generateZoneGeometry
from fastapi.testclient import TestClient
import pytest
from importlib import reload
from dggal import *
import os
from pprint import pprint
import shapely
import json
import geopandas as gpd

aoi = [[25.329803558251513, 58.634545591972696],
       [25.329803558251513, 57.99111013411327],
       [27.131561370751513, 57.99111013411327],
       [27.131561370751513, 58.634545591972696]]

non_exist_aoi = [[113.81837742963569, 22.521237932154797],
          [113.81837742963569, 22.13760392858767],
          [114.41438573041694, 22.13760392858767],
          [114.41438573041694, 22.521237932154797]]

zone_level = [5, 6, 7, 8, 9]

aoi = shapely.Polygon(aoi)
non_exist_aoi = shapely.Polygon(non_exist_aoi)
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


validation_bbox_refinement_level5 = {'ivea7h': [], 'rhealpix': []}
validation_bbox_refinement_level8 = {'ivea7h': [], 'rhealpix': []}
geoextent = GeoExtent(ll=GeoPoint(aoi.bounds[1], aoi.bounds[0]), ur=GeoPoint(aoi.bounds[3], aoi.bounds[2]))

zones_list = ivea7h.listZones(5, geoextent)
validation_bbox_refinement_level5['ivea7h'] = set(ivea7h.getZoneTextID(z) for z in zones_list)
zones_list = rhealpix.listZones(5, geoextent)
validation_bbox_refinement_level5['rhealpix'] = set(rhealpix.getZoneTextID(z) for z in zones_list)

zones_list = ivea7h.listZones(8, geoextent)
validation_bbox_refinement_level8['ivea7h'] = set(ivea7h.getZoneTextID(z) for z in zones_list)
zones_list = rhealpix.listZones(8, geoextent)
validation_bbox_refinement_level8['rhealpix'] = set(rhealpix.getZoneTextID(z) for z in zones_list)

def test_zone_query_dggrs_zones():
    os.environ['dggs_api_config'] = './dggs_api_config_testing.json'
    import pydggsapi.api
    app = reload(pydggsapi.api).app
    client = TestClient(app)

    for grid in list(cellids.keys()):
        print("Fail test case with dggs zone query ({grid} , no params)")
        response = client.get('/dggs-api/v1-pre/dggs/{grid}/zones')
        pprint(response.json())
        assert "Either bbox or parnet must be set" in response.text
        assert response.status_code == 400

        print("Fail test case with dggs zone query ({grid} , bbox with len!=4)")
        response = client.get('/dggs-api/v1-pre/dggs/{grid}/zones', params={"bbox": "2,3,4"})
        pprint(response.json())
        assert "bbox lenght is not equal to 4" in response.text
        assert response.status_code == 400

        print(f"Success test case with dggs zones query ({grid}, bbox: {aoi.bounds}, compact=False)")
        bounds = list(map(str, aoi.bounds))
        response = client.get('/dggs-api/v1-pre/dggs/{grid}/zones', params={"bbox": ",".join(bounds), 'compact_zone': False})
        pprint(response.json())
        zones = ZonesResponse(**response.json())
        return_zones_list = zones.zones
        return_zones_list.sort()
        assert len(validation_bbox_refinement_level5[grid] - set(return_zones_list)) == 0
        assert response.status_code == 200

        print(f"Success test case with dggs zones query ({grid}, bbox: {aoi.bounds}, zone_level=8, compact=False)")
        response = client.get('/dggs-api/v1-pre/dggs/{grid}/zones', params={"bbox": ",".join(bounds), 'zone_level': 8, 'compact_zone': False})
        pprint(response.json())
        zones = ZonesResponse(**response.json())
        return_zones_list = zones.zones
        return_zones_list.sort()
        assert len(validation_bbox_refinement_level8[grid] - set(return_zones_list)) == 0
        assert response.status_code == 200

        print(f"Success test case with dggs zones query ({grid}, bbox: {aoi.bounds}, zone_level=8, compact=False, geojson)")
        response = client.get('/dggs-api/v1-pre/dggs/{grid}/zones', headers={'Accept': 'Application/geo+json'},
                              params={"bbox": ",".join(bounds), 'zone_level': 8, 'compact_zone': False})
        pprint(response.json())
        zones_geojson = ZonesGeoJson(**response.json())
        return_features_list = zones_geojson.features
        geometry = [shapely.from_geojson(json.dumps(f.geometry.__dict__)) for f in return_features_list]
        zonesID = [f.properties['zoneId'] for f in return_features_list]
        return_gdf = gpd.GeoDataFrame({'name': zonesID}, geometry=geometry, crs='wgs84').set_index('name')
        validation_hexagons_gdf.sort_index(inplace=True)
        return_gdf.sort_index(inplace=True)
        assert len(return_gdf) == len(validation_hexagons_gdf)
        assert all([shapely.equals(return_gdf.iloc[i]['geometry'], validation_hexagons_gdf.iloc[i]['geometry']) for i in range(len(return_gdf))])
        assert response.status_code == 200

        print(f"Success test case with dggs zones query ({grid}, parent zone: {cellids[0]}, zone_level=8, compact=False, geojson)")
        response = client.get('/dggs-api/v1-pre/dggs/{grid}/zones', headers={'Accept': 'Application/geo+json'},
                              params={"parent_zone": cellids[0], 'zone_level': 8, 'compact_zone': False})
        pprint(response.json())
        zones_geojson = ZonesGeoJson(**response.json())
        return_features_list = zones_geojson.features
        assert response.status_code == 200

        print(f"Empty test case with dggs zones query ({grid}, bbox: {non_exist_aoi.bounds}, zone_level=8, compact=False, geojson)")
        non_exist_bounds = list(map(str, non_exist_aoi.bounds))
        response = client.get('/dggs-api/v1-pre/dggs/{grid}/zones', headers={'Accept': 'Application/geo+json'},
                              params={"bbox": ",".join(non_exist_bounds), 'zone_level': 8, 'compact_zone': False})
        assert response.status_code == 204

