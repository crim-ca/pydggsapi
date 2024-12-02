from pydantic import ValidationError
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Link, LinkTemplate, Feature
from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesRequest, ZonesResponse, ZonesGeoJson
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint

from pydggsapi.dependencies.dggs_isea7h import DggridISEA7H
from pydggsapi.models.ogc_dggs.core import _ISEA7H_zoomlevel_fromzoneId
from pydggsapi.models.hytruck_model import querySuitability

from fastapi.exceptions import HTTPException
from pys2index import S2PointIndex
import numpy as np
import geopandas as gdf
import shapely
import logging
logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.DEBUG)


def query_zones_list(dggrsId, bbox, zoom_level, limit, dggrid, compact=True, parent_zone=None, returntype='application/json', returngeometry='zone-region'):
    if (dggrsId == 'DGGRID_ISEA7H_seqnum'):
        logging.info(f'{__name__} query zones list: {bbox}, {zoom_level}, {limit}, {parent_zone}, {compact}')
        if (zoom_level == 0 and parent_zone is not None):
            logging.error(f'{__name__} query zones list, zoom level: {zoom_level} got no parent')
            raise HTTPException(status_code=500, detail=f"zoom level: {zoom_level} got no parent")
        try:
            hex_gdf = dggrid.generate_hexgrid(bbox, zoom_level)
        except Exception as e:
            logging.error(f'{__name__} query zones list, bbox: {bbox} dggrid convert failed :{e}')
            raise HTTPException(status_code=500, detail=f"{__name__} query zones list, bbox: {bbox} dggrid convert failed")
        logging.info(f'{__name__} query zones list, number of hexagons: {len(hex_gdf)}')
        try:
            if (parent_zone is not None):
                if (dggrid.data[zoom_level - 1]['Cells'] < int(parent_zone)):
                    logging.error(f'{__name__} query zones list, parent_zone: {parent_zone} is not in zoom level: {zoom_level-1}')
                    raise HTTPException(status_code=500, detail=f"parent_zone: {parent_zone} is not in zoom level: {zoom_level-1}")
                parent_centroid = dggrid.centroid_from_cellid([parent_zone], zoom_level - 1).get_coordinates()
                parent_centroid = np.stack([parent_centroid['x'].values, parent_centroid['y'].values], axis=-1)
                hex_centroids = dggrid.centroid_from_cellid(hex_gdf.index.values, zoom_level)
                hex_centroids_xy = hex_centroids.get_coordinates()
                hex_centroids_xy = np.stack([hex_centroids_xy['x'].values, hex_centroids_xy['y'].values], axis=-1)
                parent_centroid = S2PointIndex(parent_centroid)
                distance, idx = parent_centroid.query(hex_centroids_xy)
                nearest7points = np.argsort(distance)[:7]
                childern_cellids = hex_centroids.iloc[nearest7points]['name']
                logging.info(f'{__name__} query zones list, {parent_zone} childern: {childern_cellids.values}')
                hex_gdf = hex_gdf.loc[childern_cellids]
                logging.info(f'{__name__} query zones list, parent_zone filter: {len(hex_gdf)}')
        except Exception as e:
            logging.error(f'{__name__} query zones list dggrid calculation failed :{e}')
            raise HTTPException(status_code=500, detail=f"{__name__} query zones list dggrid calculation failed :{e}")
        if (len(hex_gdf) == 0):
            raise HTTPException(status_code=500, detail=f"Parent zone {parent_zone} is not with in bbox: {bbox}")
        if (compact):
            bbox_gdf = gdf.GeoDataFrame([0] * len(hex_gdf), geometry=[bbox] * len(hex_gdf), crs='wgs84')
            hex_gdf.reset_index(inplace=True)
            hex_gdf.set_crs('wgs84', inplace=True)
            not_touching = bbox_gdf.geometry.contains(hex_gdf.geometry)
            hex_gdf = hex_gdf[not_touching].set_index('name')
            logging.info(f'{__name__} query zones list, compact : {len(hex_gdf)}')
        zoneslist = hex_gdf.iloc[:limit].index.astype(str)
        returnedAreaMetersSquare = dggrid.data[zoom_level]['Area (km^2)'] * len(zoneslist) * 1000000
        logging.info(f'{__name__} query zones list, returnedAreaMetersSquare: {returnedAreaMetersSquare}')
        if (returntype == 'application/geo+json'):
            polygon = True if (returngeometry == 'zone-region') else False
            geometry_df = hex_gdf.iloc[:limit] if (polygon) else dggrid.centroid_from_cellid(zoneslist.astype(int), zoom_level).set_index('name', drop=True)
            geojson = GeoJSONPolygon if (polygon) else GeoJSONPoint
            features = [Feature(**{'type': 'Feature', 'id': i, 'geometry': geojson(**eval(shapely.to_geojson(geometry_df.loc[cellid].geometry))), 'properties': {'zoneId': cellid}}) for i, cellid in enumerate(geometry_df.index)]
            return ZonesGeoJson(**{'type': 'FeatureCollection', 'features': features})

        return ZonesResponse(**{'zones': zoneslist.values, 'returnedAreaMetersSquare': returnedAreaMetersSquare})
    else:
        raise NotImplementedError(f'zone-query (zones list) is not implemented for {dggrsId}')

