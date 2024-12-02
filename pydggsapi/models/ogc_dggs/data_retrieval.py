from pydantic import ValidationError
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Link, LinkTemplate
from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataRequest, Property, Value, ZonesDataDggsJsonResponse, Feature, ZonesDataGeoJson
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint

from pydggsapi.dependencies.dggs_isea7h import DggridISEA7H
from pydggsapi.models.hytruck_model import querySuitability

from fastapi.exceptions import HTTPException
from fastapi.responses import FileResponse
from clickhouse_driver import Client
from pys2index import S2PointIndex
from numcodecs import Blosc
from pprint import pprint
import shapely
import tempfile
import numpy as np
import zarr
import geopandas as gpd
import os
import logging
logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


def query_zone_data(zoneId, zone_level, depth, dggrsId, dggrslink, dggrid: DggridISEA7H,
                    client: Client, returntype='application/dggs-json', returngeometry='zone-region'):
    logging.info(f'{__name__} query zone data {dggrsId}, zone id: {zoneId}, depth: {depth}, return: {returntype}, geometry: {returngeometry}')
    if (dggrsId == 'DGGRID_ISEA7H_seqnum'):
        zoneId = [[zoneId]]
        polygon = True if (returngeometry == 'zone-region') else False
        # get cell ids for each zoom_level by using dggrid, also can just query from DB, it should be faster.
        # but it need an addition function since the current querySuitability won't return other's cell ids
        try:
            if (len(zone_level) > 1):
                parent_hex = dggrid.hexagon_from_cellid(zoneId[0], zone_level[0])
                parent_centroid = dggrid.centroid_from_cellid(zoneId[0], zone_level[0]).get_coordinates()
                parent_centroid = np.stack([parent_centroid['x'].values, parent_centroid['y'].values], axis=-1)
                parent_centroid = S2PointIndex(parent_centroid)
                for i, z in enumerate(zone_level[1:], 1):
                    df = dggrid.cellids_from_extent(parent_hex.geometry.values[0], z)
                    childern_centroids = dggrid.centroid_from_cellid(df[0].values, z)
                    hex_centroids_xy = childern_centroids.get_coordinates()
                    hex_centroids_xy = np.stack([hex_centroids_xy['x'].values, hex_centroids_xy['y'].values], axis=-1)
                    distance, idx = parent_centroid.query(hex_centroids_xy)
                    nearestpoints = np.argsort(distance)[:(7**(z - zone_level[0]))]
                    childern_cellids = childern_centroids.iloc[nearestpoints]['name']
                    zoneId.append(childern_cellids.tolist())
            if (depth is not None):
                if (depth[0] != 0):
                    zone_level = zone_level[1:]
                    zoneId = zoneId[1:]
        except Exception as e:
            logging.error(f'{__name__} query zone data {dggrsId}, zone id: {zoneId} dggrid calculation failed: {e}')
            raise HTTPException(status_code=500, detail='{__name__} query zone data {dggrsId}, zone id: {zoneId} dggrid calculation failed')
        properties = None
        values = {}
        logging.info(f'{__name__} query zone data {dggrsId}, zone id: {zoneId}, zone level: {zone_level}')
        zarr_root, tmpfile = None, None
        features = []
        id_ = 0
        if (returntype == 'application/zarr+zip'):
            tmpfile = tempfile.mkstemp()
            zipstore = zarr.ZipStore(tmpfile[1], mode='w')
            zarr_root = zarr.group(zipstore)
        for i, z in enumerate(zone_level):
            v, prop, _ = querySuitability(client, zoneId[i], z, filter_out=False)
            if (len(v) > 0):
                v = np.array(v)
                if (returntype == 'application/geo+json'):
                    cellid_idx = [g for g, p in enumerate(prop) if (p[0] == 'cellid')][0]
                    geometry_df = dggrid.hexagon_from_cellid(v[:, cellid_idx], z) if (polygon) else dggrid.centroid_from_cellid(v[:, cellid_idx], z)
                    if (polygon):
                        geometry_json = [GeoJSONPolygon(**eval(shapely.to_geojson(g))) for g in geometry_df.geometry]
                    else:
                        geometry_json = [GeoJSONPoint(**eval(shapely.to_geojson(g))) for g in geometry_df.geometry]
                    for a in range(v.shape[0]):
                        p = {p[0]: v[a, b] for b, p in enumerate(prop) if (p[0] != 'suitability')}
                        p['zoneId'] = int(p['cellid'])
                        del p['cellid']
                        features.append(Feature(**{'type': "Feature", 'id': id_ + a,
                                                   'geometry': geometry_json[a], 'properties': p}))
                    id_ += int(v.shape[0])
                    logging.info(f'{__name__} query zone data {dggrsId}, zone id: {zoneId}, geo+json features len: {len(features)}')
                else:
                    # application/dggs+json and application/zarr+zip
                    properties = {p[0]: Property(**{'type': p[1]}) for p in prop if (p[0] != 'cellid' and p[0] != 'suitability')}
                    if (len(values.keys()) == 0):
                        for p in properties.keys():
                            values[p] = []
                    for j, p in enumerate(prop):
                        if (p[0] != 'cellid' and p[0] != 'suitability'):
                            values[p[0]].append(Value(**{'depth': z, 'shape': {'count': len(v)}, "data": v[:, j].tolist()}))

                        if (zarr_root is not None):
                            if (p[0] != 'suitability'):
                                root = zarr_root
                                if (len(zone_level) > 1):
                                    if (f'zone_level_{z}' not in zarr_root.group_keys()):
                                        root = zarr_root.create_group(f'zone_level_{z}')
                                        root.attrs.update({k: v.__dict__ for k, v in properties.items()})
                                    else:
                                        root = zarr_root[f'zone_level_{z}']
                                compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
                                sub_zarr = root.create_dataset(f'{p[0]}_zone_level_' + str(z), data=v[:, j], compressor=compressor)
                                sub_zarr.attrs.update({'_ARRAY_DIMENSIONS': ['cell_ids']})
            else:
                raise HTTPException(status_code=204)
        if (zarr_root is not None):
            zarr_root.attrs.update({k: v.__dict__ for k, v in properties.items()})
            zarr.consolidate_metadata(zipstore)
            zipstore.close()
            return FileResponse(tmpfile[1], headers={'content-type': 'application/zarr+zip'})
        if (returntype == 'application/geo+json'):
            return ZonesDataGeoJson(**{'type': 'FeatureCollection', 'features': features})

        return_ = {'dggrs': dggrslink, 'zoneId': str(zoneId[0][0]), 'depths': zone_level,
                   'properties': properties, 'values': values}
        return ZonesDataDggsJsonResponse(**return_)
    else:
        raise NotImplementedError(f'data-retrieval (zone data) is not implemented for {dggrsId}')




