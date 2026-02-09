from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Feature, ReturnGeometryTypes
from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesResponse, ZonesGeoJson
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescription
from pydggsapi.schemas.api.collections import Collection

from pydggsapi.dependencies.dggrs_providers.abstract_dggrs_provider import AbstractDGGRSProvider
from pydggsapi.dependencies.collections_providers.abstract_collection_provider import AbstractCollectionProvider, DatetimeNotDefinedError
from pydggsapi.dependencies.api.utils import getCQLAttributes

import numpy as np
from fastapi import Response
from pygeofilter.ast import AstType
from shapely.geometry import Polygon
from typing import Dict
import logging

logger = logging.getLogger()


def query_zones_list(
    bbox: Polygon | None,
    zone_level: int,
    limit: int,
    dggrs_info: DggrsDescription,
    dggrs_provider: AbstractDGGRSProvider,
    collection: Dict[str, Collection],
    collection_provider: Dict[str, AbstractCollectionProvider],
    compact: bool = True,
    parent_zone: str | int | None = None,
    returntype: str = 'application/json',
    returngeometry: ReturnGeometryTypes = 'zone-region',
    cql_filter: AstType = None,
    include_datetime: bool = False,
) -> ZonesResponse | ZonesGeoJson | Response | None:
    logger.debug(f'{__name__} query zones list: {bbox}, {zone_level}, {limit}, {parent_zone}, {compact}')
    # generate zones for the bbox at the required zone_level
    result = dggrs_provider.zoneslist(bbox, zone_level, parent_zone, returngeometry, compact)
    filter_ = []
    cql_attributes = set() if (cql_filter is None) else getCQLAttributes(cql_filter)
    zone_list_binary = (returntype == 'application/x-binary')
    skipped = 0
    for k, v in collection.items():
        converted = None
        converted_zones = result.zones
        converted_level = zone_level
        datasource_id = v.collection_provider.datasource_id
        cp_id = v.collection_provider.providerId
        datasource_vars = list(collection_provider[cp_id].get_datadictionary(datasource_id).data.keys())
        zone_id_repr = v.collection_provider.dggrs_zoneid_repr
        intersection = (set(datasource_vars) & cql_attributes)
        # check if the cql attributes contain inside the datasource
        # The datasource of the collection must consist all columns that match with the attributes of the cql filter
        if ((len(cql_attributes) > 0)):
            if ((len(intersection) == 0) or (len(intersection) != len(cql_attributes))):
                skipped += 1
                continue
        if (v.collection_provider.dggrsId != dggrs_info.id and
                v.collection_provider.dggrsId in dggrs_provider.dggrs_conversion):
            # perform conversion
            converted = dggrs_provider.convert(result.zones, v.collection_provider.dggrsId, zone_id_repr)
            converted_zones = converted.target_zoneIds
            converted_level = converted.target_res[0]
        else:
            if (zone_id_repr != 'textual'):
                converted_zones = dggrs_provider.zone_id_from_textual(converted_zones, zone_id_repr)
        try:
            filtered_zoneIds = collection_provider[cp_id].get_data(converted_zones, converted_level,
                                                                   datasource_id, cql_filter, include_datetime,
                                                                   input_zoneIds_padding=False,
                                                                   collection_timestamp=v.timestamp).zoneIds
        except DatetimeNotDefinedError:
            filtered_zoneIds = []
            pass
        if (converted is not None):
            # The zoneId repr of target_zoneIds and the filtered_zoneIds is aligned, no need to handle
            # and the zoneIds is in original repr (str)
            filtered_zoneIds = np.array(converted.zoneIds)[np.isin(converted.target_zoneIds, filtered_zoneIds)].tolist()
        if (zone_id_repr != 'textual' and not zone_list_binary):
            filtered_zoneIds = dggrs_provider.zone_id_to_textual(filtered_zoneIds, zone_id_repr, zone_level)
        elif (zone_id_repr != 'int' and zone_list_binary):
            filtered_zoneIds = dggrs_provider.zone_id_from_textual(filtered_zoneIds, "int")
        filter_ += filtered_zoneIds
    if (skipped == len(collection)):
        raise ValueError(f"{__name__} query zones list cql attributes({cql_attributes}) not found in all collections.")
    if (len(filter_) == 0):
        return None
    logger.debug(f'{__name__} query zones list result: {len(filter_)}')
    if zone_list_binary:
        zone_ids = np.array(filter_, dtype=np.uint64)
        zone_count = np.array([len(zone_ids)], dtype=np.uint64)
        zone_list = np.concatenate((zone_count, zone_ids))
        return Response(content=zone_list.tobytes(), media_type='application/x-binary')
    if (returntype == 'application/geo+json'):
        features = [Feature(**{'type': 'Feature', 'id': i, 'geometry': result.geometry[i], 'properties': {'zoneId': zid}})
                    for i, zid in enumerate(result.zones[:limit]) if (zid in filter_)]
        return ZonesGeoJson(**{'type': 'FeatureCollection', 'features': features})
    total_area = sum(np.array(result.returnedAreaMetersSquare)[np.isin(result.zones, filter_)].tolist())
    return ZonesResponse(**{'zones': np.unique(filter_[:limit]), 'returnedAreaMetersSquare': total_area})
