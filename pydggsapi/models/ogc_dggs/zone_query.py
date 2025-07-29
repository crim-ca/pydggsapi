from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Feature
from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesResponse, ZonesGeoJson
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescription
from pydggsapi.schemas.api.collections import Collection


from pydggsapi.dependencies.dggrs_providers.abstract_dggrs_provider import AbstractDGGRSProvider
from pydggsapi.dependencies.collections_providers.abstract_collection_provider import AbstractCollectionProvider

from typing import Dict
import logging

logger = logging.getLogger()


def query_zones_list(bbox, zone_level, limit, dggrs_info: DggrsDescription, dggrs_provider: AbstractDGGRSProvider,
                     collection: Dict[str, Collection], collection_provider: Dict[str, AbstractCollectionProvider],
                     compact=True, parent_zone=None, returntype='application/json', returngeometry='zone-region'):
    logger.debug(f'{__name__} query zones list: {bbox}, {zone_level}, {limit}, {parent_zone}, {compact}')
    result = dggrs_provider.zoneslist(bbox, zone_level, parent_zone, returngeometry, compact)
    converted_zones = result.zones
    converted_level = zone_level
    filter_ = 0
    for k, v in collection.items():
        if (v.collection_provider.dggrsId != dggrs_info.id):
            converted = dggrs_provider.convert(result.zones, v.collection_provider.dggrsId)
            converted_zones = converted.target_zoneIds
            converted_level = converted.target_res[0]
        params = v.collection_provider.getdata_params
        data = collection_provider[v.collection_provider.providerId].get_data(converted_zones, converted_level, **params)
        filter_ += len(data.zoneIds)
    if (filter_ == 0):
        return None
    logger.debug(f'{__name__} query zones list result: {len(result.zones)}, returnedAreaMetersSquare: {result.returnedAreaMetersSquare}')
    if (returntype == 'application/geo+json'):
        features = [Feature(**{'type': 'Feature', 'id': i, 'geometry': geo, 'properties': {'zoneId': result.zones[i]}}) for i, geo in enumerate(result.geometry[:limit])]
        return ZonesGeoJson(**{'type': 'FeatureCollection', 'features': features})

    return ZonesResponse(**{'zones': result.zones[:limit], 'returnedAreaMetersSquare': result.returnedAreaMetersSquare})

