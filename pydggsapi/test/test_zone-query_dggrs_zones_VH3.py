from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesResponse, ZonesGeoJson
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
zone_level = [5, 6, 7, 8, 9]

aoi = shapely.Polygon(aoi)

#validation_zone_level_5_hexagons_gdf = dggrid.grid_cell_polygons_for_extent('IGEO7', 5, clip_geom=aoi, output_address_type='Z7_STRING')
#validation_hexagons_gdf = dggrid.grid_cell_polygons_for_extent('IGEO7', 8, clip_geom=aoi, output_address_type='Z7_STRING')
#validation_centroids_gdf = dggrid.grid_cell_centroids_for_extent('IGEO7', 8, clip_geom=aoi, output_address_type='Z7_STRING')
#validation_zone_level_5_hexagons_gdf.set_index('name', inplace=True)
#validation_hexagons_gdf.set_index('name', inplace=True)
#validation_centroids_gdf.set_index('name', inplace=True)


def test_zone_query_dggrs_zones_VH3():
    os.environ['dggs_api_config'] = './dggs_api_config.json'
    app = reload(pydggsapi.api).app
    client = TestClient(app)

    print(f"Success test case with dggs zones query (VH3, bbox: {aoi.bounds}, compact=False)")
    response = client.get('/dggs-api/v1-pre/dggs/VH3/zones', params={"bbox": aoi.bounds, 'compact_zone': False})
    pprint(response.json())
    zones = ZonesResponse(**response.json())
    #return_zones_list = zones.zones
    #return_zones_list.sort()
    #validation_zones_list = validation_zone_level_5_hexagons_gdf.index.values.astype(str).tolist()
    #validation_zones_list.sort()
    #assert len(validation_zones_list) == len(return_zones_list)
    #assert all([validation_zones_list[i] == z for i, z in enumerate(return_zones_list)])
    assert response.status_code == 200

    print(f"Success test case with dggs zones query (VH3, bbox: {aoi.bounds}, zone_level=8, compact=False)")
    response = client.get('/dggs-api/v1-pre/dggs/VH3/zones', params={"bbox": aoi.bounds, 'zone_level': 6, 'compact_zone': False})
    pprint(response.json())
    zones = ZonesResponse(**response.json())
    #return_zones_list = zones.zones
    #return_zones_list.sort()
    #validation_zones_list = validation_hexagons_gdf.index.values.astype(str).tolist()
    #validation_zones_list.sort()
    #assert len(validation_zones_list) == len(return_zones_list)
    #assert all([validation_zones_list[i] == z for i, z in enumerate(return_zones_list)])
    assert response.status_code == 200

    print(f"Success test case with dggs zones query (VH3, bbox: {aoi.bounds}, zone_level=8, compact=True)")
    response = client.get('/dggs-api/v1-pre/dggs/VH3/zones', params={"bbox": aoi.bounds, 'zone_level': 6, 'compact_zone': True})
    pprint(response.json())
    zones = ZonesResponse(**response.json())
    #return_zones_list = zones.zones
    #return_zones_list.sort()
    #validation_zones_list = validation_hexagons_gdf.index.values.astype(str).tolist()
    #validation_zones_list.sort()
    #assert len(validation_zones_list) == len(return_zones_list)
    #assert all([validation_zones_list[i] == z for i, z in enumerate(return_zones_list)])
    assert response.status_code == 200

    print(f"Success test case with dggs zones query (VH3, bbox: {aoi.bounds}, zone_level=8, compact=False, geojson)")
    response = client.get('/dggs-api/v1-pre/dggs/VH3/zones', headers={'Accept': 'Application/geo+json'},
                          params={"bbox": aoi.bounds, 'zone_level': 6, 'compact_zone': False})
    pprint(response.json())
    zones_geojson = ZonesGeoJson(**response.json())
    #return_features_list = zones_geojson.features
    #geometry = [shapely.from_geojson(json.dumps(f.geometry.__dict__)) for f in return_features_list]
    #zonesID = [f.properties['zoneId'] for f in return_features_list]
    #return_gdf = gpd.GeoDataFrame({'name': zonesID}, geometry=geometry, crs='wgs84').set_index('name')
    #validation_hexagons_gdf.sort_index(inplace=True)
    #return_gdf.sort_index(inplace=True)
    #assert len(return_gdf) == len(validation_hexagons_gdf)
    #assert all([shapely.equals(return_gdf.iloc[i]['geometry'], validation_hexagons_gdf.iloc[i]['geometry']) for i in range(len(return_gdf))])
    assert response.status_code == 200

    print(f"Success test case with dggs zones query (VH3, parent zone: {cellids[0]}, zone_level=8, compact=False, geojson)")
    response = client.get('/dggs-api/v1-pre/dggs/VH3/zones', headers={'Accept': 'Application/geo+json'},
                          params={"parent_zone": cellids[0], 'zone_level': 6, 'compact_zone': False})
    pprint(response.json())
    zones_geojson = ZonesGeoJson(**response.json())
    return_features_list = zones_geojson.features
    assert response.status_code == 200
