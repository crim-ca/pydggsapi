# here should be all OGC DGGS API related routes
# they should all live under e.g. /dggs-api/v1-pre
# that means this module export a FastAPI router that gets mounted
# in the main api.py under /dggs-api/v1-pre

from fastapi import APIRouter, HTTPException, Depends, Request, Path
from typing import Optional, Dict, Union

from pydggsapi.schemas.ogc_dggs.dggrs_list import DggrsListResponse
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescriptionRequest, DggrsDescription
from pydggsapi.schemas.ogc_dggs.dggrs_zones_info import ZoneInfoRequest, ZoneInfoResponse
from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataRequest, ZonesDataDggsJsonResponse, support_returntype
from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesRequest, ZonesResponse, ZonesGeoJson, zone_query_support_returntype
from pydggsapi.schemas.api.collections import Collection

from pydggsapi.models.ogc_dggs.core import query_support_dggs, query_dggrs_definition, query_zone_info, landingpage
from pydggsapi.models.ogc_dggs.data_retrieval import query_zone_data
from pydggsapi.models.ogc_dggs.zone_query import query_zones_list

from pydggsapi.dependencies.api.collections import get_collections_info
from pydggsapi.dependencies.api.collection_providers import get_collection_providers
from pydggsapi.dependencies.api.dggrs import get_dggrs_descriptions, get_dggrs_class, get_conformance_classes

from pydggsapi.dependencies.dggrs_providers.AbstractDGGRS import AbstractDGGRS

from fastapi.responses import JSONResponse, FileResponse
import logging
import copy
import pyproj
import importlib
from shapely.geometry import box
from shapely.ops import transform


logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
router = APIRouter()

dggrs, dggrs_providers, collections, collections_provider = {}, {}, {}, {}


def _import_dggrs_class(dggrsId):
    try:
        classname = get_dggrs_class(dggrsId)
        if (classname is None):
            logging.error(f'{__name__} {dggrsId} class not found.')
            raise Exception(f'{__name__} {dggrsId} class not found.')
    except Exception as e:
        logging.error(f'{__name__} {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} {e}')
    try:
        module, classname = classname.split('.') if (len(classname.split('.')) == 2) else (classname, classname)
        cls_ = getattr(importlib.import_module(f'pydggsapi.dependencies.dggrs_providers.{module}'), classname)
        return cls_()
    except Exception as e:
        logging.error(f'{__name__} {dggrsId} class: {classname} not imported, {e}')
        raise Exception(f'{__name__} {dggrsId} class: {classname} not imported, {e}')


def _import_collection_provider(providerConfig: dict):
    try:
        classname = providerConfig.classname
        params = providerConfig.initial_params
        module, classname = classname.split('.') if (len(classname.split('.')) == 2) else (classname, classname)
        cls_ = getattr(importlib.import_module(f'pydggsapi.dependencies.collections_providers.{module}'), classname)
        return cls_(params)
    except Exception as e:
        logging.error(f'{__name__} {providerConfig.classname} import failed, {e}')
        raise Exception(f'{__name__} {providerConfig.classname} import failed, {e}')


def _get_dggrs_provider(dggrsId):
    global dggrs_providers
    try:
        return dggrs_providers[dggrsId]
    except KeyError:
        logging.error(f'{__name__} : {dggrsId} not found in dggrs providers')
        raise HTTPException(status_code=500, detail=f'{__name__} : {dggrsId} not found in dggrs providers')


def _get_collection_provider(providerId):
    global collection_providers
    try:
        return collection_providers[providerId]
    except KeyError:
        logging.error(f'{__name__} : {providerId} not found in collection providers')
        raise HTTPException(status_code=500, detail=f'{__name__} : {providerId} not found in collection providers')


def _check_dggrs_description(dggrsId: str = Path(...)):
    global dggrs
    try:
        return dggrs[dggrsId]
    except KeyError as e:
        logging.error(f'{__name__} {dggrsId} not supported : {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} {dggrsId} not supported: {e}')


def _check_collection(collectionId=None):
    global collections
    if (collectionId is None):
        return collections
    try:
        return collections[collectionId]
    except KeyError:
        logging.error(f'{__name__} : {collectionId} not found')
        raise HTTPException(status_code=500, detail=f'{__name__} : {collectionId} not found')


def _get_return_type(req: Request, support_returntype, default_return='application/json'):
    returntypes = req.headers.get('accept').lower() if (req.headers.get('accept') is not None) else default_return
    returntypes = returntypes.split(',')
    intersection = [i for i in returntypes if i in support_returntype]
    returntype = intersection[0] if (len(intersection) > 0) else default_return
    return returntype


# API Initialization checking and setup.
try:
    dggrs = get_dggrs_descriptions()
    collections = get_collections_info()
    collections_provider = get_collection_providers()
except Exception as e:
    logging.error(f'{__name__} {e}')
    raise Exception(f'{__name__} {e}')

for dggrsId in dggrs.keys():
    dggrs_providers[dggrsId] = _import_dggrs_class(dggrsId)

for providerId, providerConfig in collections_provider.items():
    collections_provider[providerId] = _import_collection_provider(providerConfig)


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
                       collection: Dict[str, Collection] = Depends(_check_collection)):
    logging.info(f'{__name__} called.')
    global dggrs, dggrs_providers
    selected_dggrs = copy.deepcopy(dggrs)
    try:
        if (collectionId is not None):
            selected_dggrs = {collection.collection_provider.dggrsId: selected_dggrs[collection.collection_provider.dggrsId]}
            for k, v in dggrs_providers.items():
                if (collection.collection_provider.dggrsId in v.dggrs_conversion):
                    selected_dggrs[k] = dggrs[k]
        result = query_support_dggs(req.url, selected_dggrs)
    except Exception as e:
        logging.error(f'{__name__} dggrs-list failed: {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} dggrs-list failed: {e}')
    return result


# dggrs description
@router.get("/dggs/{dggrsId}", response_model=DggrsDescription, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs/{dggrsId}", response_model=DggrsDescription, tags=['ogc-dggs-api'],
            dependencies=[Depends(_check_collection)])
async def dggrs_description(req: Request, dggrs_req: DggrsDescriptionRequest = Depends(),
                            dggrs_description: DggrsDescription = Depends(_check_dggrs_description)):
    current_url = str(req.url)
    return query_dggrs_definition(current_url, dggrs_description)


# zone-info
@router.get("/dggs/{dggrsId}/zones/{zoneId}",  response_model=ZoneInfoResponse, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs/{dggrsId}/zones/{zoneId}", response_model=ZoneInfoResponse, tags=['ogc-dggs-api'])
async def dggrs_zone_info(req: Request, zoneinfoReq: ZoneInfoRequest = Depends(),
                          dggrs_descrption: DggrsDescription = Depends(_check_dggrs_description),
                          dggrs_provider: AbstractDGGRS = Depends(_get_dggrs_provider),
                          collections: Dict[str, Collection] = Depends(_check_collection)):
    try:
        info = query_zone_info(zoneinfoReq, req.url, dggrs_descrption, dggrs_provider)
    except Exception as e:
        logging.error(f'{__name__} query zone info fail: {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} query zone info fail: {e}')
    return info


# Zone query conformance class

@router.get("/dggs/{dggrsId}/zones", response_model=Union[ZonesResponse, ZonesGeoJson], tags=['ogc-dggs-api'])
async def list_dggrs_zones(req: Request, zonesReq: ZonesRequest = Depends(),
                           dggrs_descrption: DggrsDescription = Depends(_check_dggrs_description),
                           dggrid: AbstractDGGRS = Depends(_import_dggrs_class)):

    returntype = _get_return_type(req, zone_query_support_returntype, 'application/json')
    returngeometry = zonesReq.geometry if (zonesReq.geometry is not None) else 'zone-region'
    zone_level = zonesReq.zone_level if (zonesReq.zone_level is not None) else dggrs_descrption.defaultDepth
    compact_zone = zonesReq.compact_zone if (zonesReq.compact_zone is not None) else True
    limit = zonesReq.limit if (zonesReq.limit is not None) else 100000
    parent_zone = zonesReq.parent_zone
    bbox = zonesReq.bbox

    if (parent_zone is not None):
        parent_level = dggrid.get_cells_zone_level([parent_zone])[0]
        if (parent_level > zone_level):
            logging.error(f'{__name__} query zones list, parent level({parent_level}) > zone level({zone_level})')
            raise HTTPException(status_code=500, detail=f"query zones list, parent level({parent_level}) > zone level({zone_level})")
    if (bbox is not None):
        try:
            bbox = box(*bbox)
            bbox_crs = zonesReq.bbox_crs if (zonesReq.bbox_crs is not None) else "wgs84"
            if (bbox_crs != 'wgs84'):
                logging.info(f'{__name__} query zones list {zonesReq.dggrsId}, original bbox: {bbox}')
                project = pyproj.Transformer.from_crs(bbox_crs, "wgs84", always_xy=True).transform
                bbox = transform(project, bbox)
                logging.info(f'{__name__} query zones list {zonesReq.dggrsId}, transformed bbox: {bbox}')
        except Exception as e:
            logging.error(f'{__name__} query zones list, bbox converstion failed : {e}')
            raise HTTPException(status_code=500, detail=f"{__name__} query zones list, bbox converstion failed : {e}")
    try:
        result = query_zones_list(bbox, zone_level, limit, dggrid, compact_zone, zonesReq.parent_zone, returntype, returngeometry)
        return result
    except Exception as e:
        logging.error(f'{__name__} query zones list failed: {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} query zones list failed: {e}')

# Data-retrieval conformance class


@router.get("/dggs/{dggrsId}/zones/{zoneId}/data", response_model=None, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs/{dggrsId}/zones/{zoneId}/data", response_model=ZoneInfoResponse, tags=['ogc-dggs-api'])
async def dggrs_zones_data(req: Request, zonedataReq: ZonesDataRequest = Depends(),
                           dggrs_info: DggrsDescription = Depends(_check_dggrs_description), dggrid: AbstractDGGRS = Depends(_import_dggrs_class),
                           collections=Depends(_check_collection)) -> ZonesDataDggsJsonResponse | FileResponse:

    returntype = _get_return_type(req, support_returntype, 'application/json')
    zoneId = zonedataReq.zoneId
    depth = zonedataReq.depth
    returngeometry = zonedataReq.geometry if (zonedataReq.geometry is not None) else 'zone-region'
    zone_level = []
    exclude = True if depth is not None else False
    # prepare zone levels from zoneId + depth
    # The first element of zone_level will be the zoneId's level, follow by the required relative depth (zoneId's level + d)
    try:
        zone_level.append(dggrid.get_cells_zone_level([zoneId])[0])
    except Exception as e:
        logging.error(f'{__name__} query zone data {zonedataReq.dggrsId}, zone id {zoneId} get zone level error: {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} query zone data {zonedataReq.dggrsId}, zone id {zoneId} get zone level error: {e}')
    if (depth is not None):
        if (len(depth) == 2):
            exclude = False if depth[0] == 0 else exclude
            depth = list(range(depth[0], depth[1] + 1))
        zone_level = zone_level + [zone_level[0] + d for d in depth if d > 0]
    for z in zone_level:
        if (z > dggrs_info.maxRefinementLevel):
            logging.error(f'{__name__} query zone data {zonedataReq.dggrsId}, zone id {zoneId} with relative depth: {z} not supported')
            raise HTTPException(status_code=500,
                                detail=f"query zone data {zonedataReq.dggrsId}, zone id {zoneId} with relative depth: {z} not supported")
    collections_provider = [_import_collection_provider(c) for c in collections]
    link = [link.href for link in dggrs_info.links if (link.rel == 'ogc-rel:dggrs-definition')][0]
    try:
        result = query_zone_data(zoneId, zone_level, zonedataReq.dggrsId, link,
                                 dggrid, collections_provider, returntype, returngeometry, exclude)
        return result
    except Exception as e:
        logging.error(f'{__name__} data_retrieval failed: {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} data_retrieval failed: {e}')



