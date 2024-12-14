# here should be all OGC DGGS API related routes
# they should all live under e.g. /dggs-api/v1-pre
# that means this module export a FastAPI router that gets mounted
# in the main api.py under /dggs-api/v1-pre

from fastapi import APIRouter, HTTPException, Depends, Request, Path
from typing import Optional, Dict

from pydggsapi.schemas.ogc_dggs.dggrs_list import DggrsListResponse
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescriptionRequest, DggrsDescription
from pydggsapi.schemas.ogc_dggs.dggrs_zones_info import ZoneInfoRequest, ZoneInfoResponse
from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataRequest, ZonesDataDggsJsonResponse, support_returntype
from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesRequest, ZonesResponse, ZonesGeoJson, zone_query_support_returntype
from pydggsapi.schemas.api.config import Collection

from pydggsapi.models.ogc_dggs.core import query_support_dggs, query_dggrs_definition, query_zone_info, landingpage
from pydggsapi.models.ogc_dggs.data_retrieval import query_zone_data
from pydggsapi.models.ogc_dggs.zone_query import query_zones_list

from pydggsapi.dependencies.db import get_database_client, get_conformance_classes
from pydggsapi.dependencies.config.collections import get_collections_info
from pydggsapi.dependencies.config.dggrs_indexes import get_dggrs_items, get_dggrs_descriptions, get_dggrs_class
from pydggsapi.dependencies.dggrs_providers.AbstractDGGRS import AbstractDGGRS

from fastapi.responses import JSONResponse, FileResponse
import logging
import pyproj
import importlib
from shapely.geometry import box
from shapely.ops import transform


logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
router = APIRouter()


def _check_dggrs_description_exists(dggrs_descrptions: Dict[str, DggrsDescription], dggrsId):
    if (dggrs_descrptions is None):
        logging.error(f'{__name__} : No dggrs definition is found.')
        raise HTTPException(status_code=500, detail=f'{__name__} : No dggrs definition is found.')
    if (dggrsId not in dggrs_descrptions.keys()):
        logging.error(f'{__name__} : {dggrsId} not supported.')
        raise HTTPException(status_code=500, detail=f'{__name__} : {dggrsId} not supported')


def _check_collection_exists(collections: Dict[str, Collection], collectionId, dggrsId=None):
    if (collections is None):
        logging.error(f'{__name__} collection: No collections definition are found.')
        raise HTTPException(status_code=500, detail=f'{__name__} collection: No collections definition are found.')
    if (collectionId not in list(collections.keys())):
        logging.error(f'{__name__} collection: {collectionId} not found')
        raise HTTPException(status_code=500, detail=f'{__name__} collection: {collectionId} not found')
    if (dggrsId is not None):
        if (dggrsId not in collections[collectionId].dggrs_indexes.keys()):
            logging.error(f'{__name__} collection: {collectionId} not support with {dggrsId}')
            raise HTTPException(status_code=500, detail=f'{__name__} collection: {collectionId} not support with {dggrsId}')


def _get_return_type(req: Request, support_returntype, default_return='application/json'):
    returntypes = req.headers.get('accept').lower() if (req.headers.get('accept') is not None) else default_return
    returntypes = returntypes.split(',')
    intersection = [i for i in returntypes if i in set(support_returntype)]
    returntype = intersection[0] if (len(intersection) > 0) else default_return
    return returntype


async def _get_dggrs_class(dggrs_id: str = Path(...)):
    try:
        classname = get_dggrs_class(dggrs_id)
    except Exception as e:
        logging.error(f'{__name__} {dggrs_id} {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} {dggrs_id} {e}')
    if (classname is None):
        logging.error(f'{__name__} {dggrs_id} not supported.')
        raise HTTPException(status_code=500, detail=f'{__name__} {dggrs_id} not supported.')
    try:
        cls_ = getattr(importlib.import_module(f'pydggsapi.dependencies.dggrs_providers.{classname}'), classname)
        return cls_()
    except Exception as e:
        logging.error(f'{__name__} {dggrs_id} class: {classname} not imported, {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} {dggrs_id} class: {classname} not imported, {e}')

# Landing page and conformance


@router.get("/", tags=['ogc-dggs-api'])
async def landing_page(req: Request):
    return landingpage(req.url)


@router.get("/conformance", tags=['ogc-dggs-api'])
async def conformance(conformance_classes=Depends(get_conformance_classes)):
    return JSONResponse(content={'conformsTo': conformance_classes})


# Core conformance class

# dggrs-list
@router.get("/dggs", response_model=DggrsListResponse, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs", response_model=DggrsListResponse, tags=['ogc-dggs-api'])
async def support_dggs(req: Request, collectionId: Optional[str] = None,
                       collections=Depends(get_collections_info), dggrs_items=Depends(get_dggrs_items)):
    logging.info(f'{__name__} called.')
    if (dggrs_items is None):
        logging.error(f'{__name__} dggs-list: No dggrs definition is found.')
        raise HTTPException(status_code=500, detail=f'{__name__} dggs-list: No dggrs definition is found.')
    filter_ = list(dggrs_items.keys())
    if (collectionId is not None):
        _check_collection_exists(collections, collectionId)
        filter_ = [dggrsid for dggrsid in collections[collectionId].dggrs_indexes.keys()]
    return query_support_dggs(req.url, dggrs_items, filter_)


# dggrs description
@router.get("/dggs/{dggrs_id}", response_model=DggrsDescription, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs/{dggrs_id}", response_model=DggrsDescription, tags=['ogc-dggs-api'])
async def dggrs_description(req: Request, dggrs_req: DggrsDescriptionRequest = Depends(),
                            collections=Depends(get_collections_info), dggrs_descriptions=Depends(get_dggrs_descriptions)):
    _check_dggrs_description_exists(dggrs_descriptions, dggrs_req.dggrs_id)
    current_url = str(req.url)
    if (dggrs_req.collectionId is not None):
        _check_collection_exists(collections, dggrs_req.collectionId, dggrs_req.dggrs_id)
    return query_dggrs_definition(current_url, dggrs_descriptions[dggrs_req.dggrs_id])


# zone-info
@router.get("/dggs/{dggrs_id}/zones/{zoneId}",  response_model=ZoneInfoResponse, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs/{dggrs_id}/zones/{zoneId}", response_model=ZoneInfoResponse, tags=['ogc-dggs-api'])
async def dggrs_zone_info(req: Request, zoneinfoReq: ZoneInfoRequest = Depends(), dggrid: AbstractDGGRS = Depends(_get_dggrs_class),
                          collections=Depends(get_collections_info), dggrs_descrptions=Depends(get_dggrs_descriptions)):
    _check_dggrs_description_exists(dggrs_descrptions, zoneinfoReq.dggrs_id)
    if (zoneinfoReq.collectionId is not None):
        _check_collection_exists(collections, zoneinfoReq.collectionId, zoneinfoReq.dggrs_id)
    try:
        info = query_zone_info(zoneinfoReq, req.url, dggrs_descrptions[zoneinfoReq.dggrs_id], dggrid)
    except Exception as e:
        logging.error(f'{__name__} dggs-zoneinfo: {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} dggs-zoneifno: {e}')
    return info


# Zone query conformance class

@router.get("/dggs/{dggrs_id}/zones", response_model=ZonesResponse | ZonesGeoJson, tags=['ogc-dggs-api'])
async def list_dggrs_zones(req: Request, zonesReq: ZonesRequest = Depends(),
                           dggrid=Depends(get_dggrs_class), dggs_info=Depends(get_dggrs_descriptions)):
    returntype = _get_return_type(req, zone_query_support_returntype, 'application/json')
    returngeometry = zonesReq.geometry if (zonesReq.geometry is not None) else 'zone-region'
    dggs_info = dggs_info[zonesReq.dggrs_id]
    zone_level = zonesReq.zone_level if (zonesReq.zone_level is not None) else dggs_info['defaultDepth']
    compact_zone = zonesReq.compact_zone if (zonesReq.compact_zone is not None) else True
    limit = zonesReq.limit if (zonesReq.limit is not None) else 100000
    parent_zone = zonesReq.parent_zone
    if (parent_zone is not None):
        parent_level = dggrid.get_cell_zone_level([parent_zone])[0]
        if (parent_level > zone_level):
            logging.error(f'{__name__} query zones list, parent level({parent_level}) > zone level({zone_level})')
            raise HTTPException(status_code=500, detail=f"query zones list, parent level({parent_level}]) > zone level({zone_level})")
    if (zonesReq.bbox is not None):
        bbox = box(*zonesReq.bbox)
        bbox_crs = zonesReq.bbox_crs if (zonesReq.bbox_crs is not None) else "wgs84"
        if (bbox_crs != 'wgs84'):
            logging.info(f'{__name__} query zones list {zonesReq.dggrs_id}, original bbox: {bbox}')
            project = pyproj.Transformer.from_crs(bbox_crs, "wgs84", always_xy=True).transform
            bbox = transform(project, bbox)
            logging.info(f'{__name__} query zones list {zonesReq.dggrs_id}, transformed bbox: {bbox}')
    return query_zones_list(bbox, zone_level, limit, dggrid, compact_zone,
                            zonesReq.parent_zone, returntype, returngeometry)


# Data-retrieval conformance class


@router.get("/dggs/{dggrs_id}/zones/{zoneId}/data", response_model=None, tags=['ogc-dggs-api'])
async def dggrs_zones_data(req: Request, zonedataReq: ZonesDataRequest = Depends(),
                           dggrid=Depends(), dggs_info=Depends(),
                           client=Depends(get_database_client)) -> ZonesDataDggsJsonResponse | FileResponse:

    returntype = _get_return_type(req, support_returntype, 'application/json')
    zoneId = zonedataReq.zoneId
    depth = zonedataReq.depth
    returngeometry = zonedataReq.geometry if (zonedataReq.geometry is not None) else 'zone-region'
    zone_level = []
    dggs_info = dggs_info[zonedataReq.dggrs_id]
    if (zonedataReq.dggrs_id == 'DGGRID_ISEA7H_seqnum'):
        if (zoneId > dggrid.data[15]['Cells']):
            logging.error(f'{__name__} query zone data {zonedataReq.dggrs_id}, zone id {zonedataReq.zoneId} > max Zoomlevel number of zones')
            raise HTTPException(status_code=500, detail=f'{__name__} query zone info {zonedataReq.dggrs_id}, zone id {zonedataReq.zoneId} > max Zoomlevel number of zones')
        # zone_level = [_ISEA7H_zoomlevel_fromzoneId(zoneId, dggrid)]
        if (depth is not None):
            if (len(depth) == 2):
                depth = range(depth[0], depth[1] + 1)
            zone_level = zone_level + [zone_level[0] + d for d in depth if (d > 0)]
        for z in zone_level:
            if (z > dggs_info['maxRefinementLevel']):
                logging.error(f'{__name__} query zone data {zonedataReq.dggrs_id}, zone id {zonedataReq.zoneId} with relative depth: {z} not support')
                raise HTTPException(status_code=500, detail=f"zone id {zonedataReq.zoneId} with relative depth: {z} not supported")
        # get cell ids for each zoom_level by using dggrid, also can just query from DB, it should be faster.
        # but it need an addition function since the current querySuitability won't return other's cell ids
        return query_zone_data(zoneId, zone_level, depth, zonedataReq.dggrs_id, dggs_info['description_link'],
                               dggrid, client, returntype, returngeometry)
    else:
        raise NotImplementedError(f'data-retrieval (zone data) is not implemented for {zonedataReq.dggrs_id}')


