from pydantic import ValidationError
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Link, LinkTemplate, Feature
from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesRequest, ZonesResponse, ZonesGeoJson
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint

from pydggsapi.dependencies.dggrs_providers.abstract_dggrs_provider import AbstractDGGRSProvider

from fastapi.exceptions import HTTPException
import numpy as np
import geopandas as gdf
import shapely
import logging
logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.DEBUG)


def query_zones_list(bbox, zone_level, limit, dggrid: AbstractDGGRSProvider,
                     compact=True, parent_zone=None, returntype='application/json', returngeometry='zone-region'):
    logging.info(f'{__name__} query zones list: {bbox}, {zone_level}, {limit}, {parent_zone}, {compact}')
    result = dggrid.zoneslist(bbox, zone_level, parent_zone, returngeometry, compact)
    logging.info(f'{__name__} query zones list result: {len(result.zones)}, returnedAreaMetersSquare: {result.returnedAreaMetersSquare}')
    if (returntype == 'application/geo+json'):
        features = [Feature(**{'type': 'Feature', 'id': i, 'geometry': geo, 'properties': {'zoneId': result.zones[i]}}) for i, geo in enumerate(result.geometry[:limit])]
        return ZonesGeoJson(**{'type': 'FeatureCollection', 'features': features})

    return ZonesResponse(**{'zones': result.zones[:limit], 'returnedAreaMetersSquare': result.returnedAreaMetersSquare})

