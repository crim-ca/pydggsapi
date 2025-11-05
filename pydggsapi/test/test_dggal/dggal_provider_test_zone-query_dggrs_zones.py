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

app = Application(appGlobals=globals())
pydggal_setup(app)

ivea7h = IVEA7H()
rhealpix = rHEALPix()

ivea_validation_gpd = gpd.GeoDataFrame(columns=[''])

geoextent = GeoExtent(ll=GeoPoint(aoi.bounds[1], aoi.bounds[0]), ur=GeoPoint(aoi.bounds[3], aoi.bounds[2]))


zones_list = ivea7h.listZones(7, geoextent)
validation_zones_text_ids = [ivea7h.getZoneTextID(z) for z in zones_list]
parent_zone_id_rf_5 = ivea7h.getZoneTextID(612489549322387837)

validation_hexagons = []
for zone in zones_list:
    polygon = generateZoneGeometry(ivea7h, zone, None, False)
    start_vertex = polygon.coordinates[0][0]
    polygon.coordinates[0].append(start_vertex)
    validation_hexagons.append(shapely.from_geojson(json.dumps(polygon.__dict__)))

validation_hexagons = gpd.GeoDataFrame({'zone_id': validation_zones_text_ids}, geometry=validation_hexagons, crs='wgs84').set_index("zone_id")


def test_zone_query_dggrs_zones():
    os.environ['dggs_api_config'] = './dggs_api_config_testing.json'
    import pydggsapi.api
    app = reload(pydggsapi.api).app
    client = TestClient(app)
    grid = 'ivea7h'
    bounds = list(map(str, aoi.bounds))
    non_exist_bounds = list(map(str, non_exist_aoi.bounds))
    print("Fail test case with dggs zone query ({grid} , no params)")
    response = client.get('/dggs-api/v1-pre/dggs/{grid}/zones')
    assert "Either bbox or parent-zone must be set" in response.text
    assert response.status_code == 400

    print(f"Fail test case with dggs zone query ({grid} , bbox with len!=4)")
    response = client.get('/dggs-api/v1-pre/dggs/{grid}/zones', params={"bbox": "2,3,4"})
    assert "bbox length is not equal to 4" in response.text
    assert response.status_code == 400

    print(f"Success test case with dggs zones query ({grid}, bbox: {aoi.bounds}, zone_level=7, compact=False)")
    response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones', params={"bbox": ",".join(bounds), 'zone_level': 7, 'compact_zone': False})
    zones = ZonesResponse(**response.json())
    return_zones_list = zones.zones
    assert len(set(validation_zones_text_ids) - set(return_zones_list)) == 0
    assert response.status_code == 200

    print(f"Success test case with dggs zones query ({grid}, bbox: {aoi.bounds}, zone_level=7, compact=False, geojson)")
    response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones', headers={'Accept': 'Application/geo+json'},
                          params={"bbox": ",".join(bounds), 'zone_level': 7, 'compact_zone': False})
    zones_geojson = ZonesGeoJson(**response.json())
    return_features_list = zones_geojson.features
    return_geometry = [f.geometry for f in return_features_list]
    geometry = []
    for g in return_geometry:
        start_vertex = g.coordinates[0][0]
        g.coordinates[0].append(start_vertex)
        geometry.append(shapely.from_geojson(json.dumps(g.__dict__)))
    zonesID = [f.properties['zoneId'] for f in return_features_list]
    return_gdf = gpd.GeoDataFrame({'zone_id': zonesID}, geometry=geometry, crs='wgs84').set_index('zone_id')
    validation_hexagons.sort_index(inplace=True)
    return_gdf.sort_index(inplace=True)
    assert len(return_gdf) == len(validation_hexagons)
    assert all([shapely.equals(return_gdf.iloc[i]['geometry'], validation_hexagons.iloc[i]['geometry']) for i in range(len(return_gdf))])
    assert response.status_code == 200

    print(f"Success test case with dggs zones query ({grid}, parent zone: {parent_zone_id_rf_5}, zone_level=7, compact=False, geojson)")
    response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones', headers={'Accept': 'Application/geo+json'},
                          params={"parent_zone": parent_zone_id_rf_5, 'zone_level': 7, 'compact_zone': False})
    zones_geojson = ZonesGeoJson(**response.json())
    return_features_list = zones_geojson.features
    assert response.status_code == 200

    print(f"Empty test case with dggs zones query ({grid}, bbox: {non_exist_aoi.bounds}, zone_level=7, compact=False, geojson)")
    non_exist_bounds = list(map(str, non_exist_aoi.bounds))
    response = client.get(f'/dggs-api/v1-pre/dggs/{grid}/zones', headers={'Accept': 'Application/geo+json'},
                          params={"bbox": ",".join(non_exist_bounds), 'zone_level': 7, 'compact_zone': False})
    assert response.status_code == 204

