# here should be all OGC DGGS API related routes
# they should all live under e.g. /dggs-api/v1-pre
# that means this module export a FastAPI router that gets mounted
# in the main api.py under /dggs-api/v1-pre

from fastapi import APIRouter, HTTPException, Depends, Request, Path
from typing import Optional, Dict, Union

from pydggsapi.schemas.ogc_dggs.dggrs_list import DggrsListResponse
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescriptionRequest, DggrsDescription
from pydggsapi.schemas.ogc_dggs.dggrs_zones_info import ZoneInfoRequest, ZoneInfoResponse
from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataRequest, ZonesDataDggsJsonResponse, ZonesDataGeoJson, support_returntype
from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesRequest, ZonesResponse, ZonesGeoJson, zone_query_support_returntype
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import ApiCollections, ApiCollection, Link
from pydggsapi.schemas.api.collections import Collection
from pydggsapi.schemas.ogc_collections.collections import Collections as ogc_Collections
from pydggsapi.schemas.ogc_collections.collections import CollectionDesc


from pydggsapi.models.ogc_dggs.core import query_support_dggs, query_dggrs_definition, query_zone_info, landingpage
from pydggsapi.models.ogc_dggs.data_retrieval import query_zone_data
from pydggsapi.models.ogc_dggs.zone_query import query_zones_list

from pydggsapi.dependencies.api.collections import get_collections_info
from pydggsapi.dependencies.api.collection_providers import get_collection_providers
from pydggsapi.dependencies.api.dggrs import get_dggrs_descriptions, get_dggrs_class, get_conformance_classes

from pydggsapi.dependencies.dggrs_providers.abstract_dggrs_provider import AbstractDGGRSProvider
from pydggsapi.dependencies.collections_providers.abstract_collection_provider import AbstractCollectionProvider

from fastapi.responses import JSONResponse, FileResponse, Response
import logging
import copy
import pyproj
import importlib
from shapely.geometry import box
from shapely.ops import transform


logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
router = APIRouter()

dggrs, dggrs_providers, collections, collection_providers = {}, {}, {}, {}


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
        logging.error(f'{__name__} _get_dggrs_provider: {dggrsId} not found in dggrs providers')
        raise HTTPException(status_code=500, detail=f'{__name__} _get_dggrs_provider: {dggrsId} not found in dggrs providers')


def _get_collection_provider(providerId=None):
    global collection_providers
    if (providerId is None):
        return collection_providers
    try:
        return {providerId: collection_providers[providerId]}
    except KeyError:
        logging.error(f'{__name__} _get_collection_provider: {providerId} not found in collection providers')
        raise HTTPException(status_code=500, detail=f'{__name__} _get_collection_provider: {providerId} not found in collection providers')


def _get_dggrs_description(dggrsId: str = Path(...)):
    global dggrs
    try:
        return dggrs[dggrsId]
    except KeyError as e:
        logging.error(f'{__name__} {dggrsId} not supported : {e}')
        raise HTTPException(status_code=400, detail=f'{__name__}  _get_dggrs_description failed:  {dggrsId} not supported: {e}')


def _get_collection(collectionId=None, dggrsId=None):
    global collections, dggrs_providers
    if (collectionId is None):
        return collections
    try:
        c = {collectionId: collections[collectionId]}
    except KeyError:
        logging.error(f'{__name__} : {collectionId} not found')
        raise HTTPException(status_code=400, detail=f'{__name__}  _get_collection failed: {collectionId} not found')
    collection_dggrs = c[collectionId].collection_provider.dggrsId
    if (dggrsId is not None):
        _get_dggrs_description(dggrsId)
        if (collection_dggrs != dggrsId):
            if (collection_dggrs not in dggrs_providers[dggrsId].dggrs_conversion.keys()):
                raise HTTPException(status_code=400, detail=f"{__name__} _get_collection failed: collection don't support {dggrsId}.")
    return c


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
    collection_providers = get_collection_providers()
except Exception as e:
    logging.error(f'{__name__} {e}')
    raise Exception(f'{__name__} {e}')

# check if dggrs and collection providerID defined in collections are exists
c1 = set([v.collection_provider.dggrsId for k, v in collections.items()]) <= set(dggrs.keys())
c2 = set([v.collection_provider.providerId for k, v in collections.items()]) <= set(collection_providers.keys())
if (c1 is False or c2 is False):
    logging.error(f'{__name__} collection_provider: either collection providerId or dggrsId not exists ')
    raise Exception(f'{__name__} collection_provider: either collection providerId or dggrsId not exists ')

for dggrsId in dggrs.keys():
    dggrs_providers[dggrsId] = _import_dggrs_class(dggrsId)

for providerId, providerConfig in collection_providers.items():
    collection_providers[providerId] = _import_collection_provider(providerConfig)



# Collection landing page
@router.get("/collections", response_model=ogc_Collections, tags=['ogc-collections-api'])
async def list_collections(req: Request, collection: Dict[str, Collection] = Depends(_get_collection)):
    collection_list = []
    req.url
    for k, v in collection.items():
        self_link = Link(**{'href': str(req.url) + f'/{v.collectionid}', 'rel': 'self', 'type': 'application/json',
                            'title': f'General Description of {v.collectionid}'})
        dggs_list_link = Link(**{'href': str(req.url) + f'/{v.collectionid}/dggs', 'rel': 'ogc-rel:dggrs-list',
                                 'type': 'application/json', 'title': 'Available DGGRS for {v.collectionid}'})
        collection_list.append(CollectionDesc(id=v.collectionid, title=v.title, description=v.description, links=[self_link, dggs_list_link]))
    self_link = Link(**{'href': str(req.url) + '/collections.json', 'rel': 'self', 'type': 'application/json',
                        'title': 'collections api'})
    return ogc_Collections(links=[self_link], collections=collection_list)

# Landing page and conformance
@router.get("/", tags=['ogc-dggs-api'])
async def landing_page(req: Request):
    return landingpage(req.url)


@router.get("/collections", tags=['ogc-dggs-api'])
async def list_collections(req: Request, response_model=ApiCollections):
    collectionsResponse = ApiCollections(
        links=[
            Link(
                href=f"{req.url}",
                rel="self",
                type="application/json",
                title="this document"
            )
        ],
        collections=[]
    )
    try:
        collections_info = _get_collection()
        # logging.info(f'{collections_info.keys()}')
        for collectionId, collection in collections_info.items():
            # logging.info(f'{dir(collection)}')
            collection_links = [
                Link(
                    href=f"{req.url}/{collectionId}",
                    rel="self",
                    type="application/json",
                    title="this document"
                ),
                Link(
                    href=f"{req.url}/{collectionId}/dggs",
                    rel="dggs",
                    type="application/json",
                    title="DGGS list"
                )
            ]
            new_collection = ApiCollection(
                    id=collectionId,
                    title=collection.title,
                    description=collection.description,
                    links=collection_links,
                    # extent=collection.extent,
                    # itemType=collection.itemType,
                    # crs=collection.crs,
                    # dggsInfo
                )
            collectionsResponse.collections.append(new_collection)
    except Exception as e:
        logging.error(f'{__name__} {e}')
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f'{__name__} {e}')

    # return JSONResponse(content=collectionsResponse)
    return collectionsResponse


@router.get("/collections/{collectionId}", tags=['ogc-dggs-api'])
async def list_collection_by_id(collectionId: str, req: Request, response_model=ApiCollection):

    collections_info = _get_collection()
    # logging.info(f'{collections_info.keys()}')
    if collectionId in collections_info.keys():
        collection = collections_info[collectionId]
        collection_links = [
            Link(
                href=f"{req.url}",
                rel="self",
                type="application/json",
                title="this document"
            ),
            Link(
                href=f"{req.url}/dggs",
                rel="dggs",
                type="application/json",
                title="DGGS list"
            )
        ]
        dggrs_info = _get_dggrs_description(collection.collection_provider.dggrsId)
        new_collection = ApiCollection(
            id=collectionId,
            title=collection.title,
            description=collection.description,
            links=collection_links,
            # extent=collection.extent,
            # itemType=collection.itemType,
            # crs=collection.crs,
            # dggsInfo=collection.dggrs_indexes
            dggsInfo=dggrs_info.__dict__

        )

        return new_collection
    else:
        raise HTTPException(status_code=404, detail=f'{__name__} {collectionId} not found')


@router.get("/conformance", tags=['ogc-dggs-api'])
async def conformance(conformance_classes=Depends(get_conformance_classes)):
    return JSONResponse(content={'conformsTo': conformance_classes})


# Core conformance class

# dggrs-list
@router.get("/dggs", response_model=DggrsListResponse, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs", response_model=DggrsListResponse, tags=['ogc-dggs-api'])
async def support_dggs(req: Request, collectionId: Optional[str] = None,
                       collection: Dict[str, Collection] = Depends(_get_collection)):
    logging.info(f'{__name__} called.')
    global dggrs, dggrs_providers
    selected_dggrs = copy.deepcopy(dggrs)
    try:
        if (collectionId is not None):
            collection = collection[collectionId]
            dggrsId = collection.collection_provider.dggrsId
            selected_dggrs = {dggrsId: selected_dggrs[dggrsId]}
            selected_dggrs[dggrsId].maxRefinementLevel = collection.collection_provider.maxzonelevel
            # find other dggrs provider support for conversion
            for k, v in dggrs_providers.items():
                if (dggrsId in v.dggrs_conversion.keys()):
                    selected_dggrs[k] = dggrs[k]
                    selected_dggrs[k].maxRefinementLevel = collection.collection_provider.maxzonelevel - v.dggrs_conversion[dggrsId].zonelevel_offset
        result = query_support_dggs(req.url, selected_dggrs)
    except Exception as e:
        logging.error(f'{__name__} dggrs-list failed: {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} dggrs-list failed: {e}')
    return result


# dggrs description
@router.get("/dggs/{dggrsId}", response_model=DggrsDescription, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs/{dggrsId}", response_model=DggrsDescription, tags=['ogc-dggs-api'],
            dependencies=[])
async def dggrs_description(req: Request, dggrs_req: DggrsDescriptionRequest = Depends(),
                            dggrs_description: DggrsDescription = Depends(_get_dggrs_description),
                            collection: Dict[str, Collection] = Depends(_get_collection),
                            dggrs_provider=Depends(_get_dggrs_provider)):
    current_url = str(req.url)
    if (dggrs_req.collectionId is not None):
        collection = collection[dggrs_req.collectionId]
        dggrs_description.maxRefinementLevel = collection.collection_provider.maxzonelevel
        # update the maxRefinementLevel if it is belongs to dggrs conversion
        if (dggrs_req.dggrsId != collection.collection_provider.dggrsId):
            dggrs_description.maxRefinementLevel += dggrs_provider.dggrs_conversion[collection.collection_provider.dggrsId].zonelevel_offset
    return query_dggrs_definition(current_url, dggrs_description)


# zone-info
@router.get("/dggs/{dggrsId}/zones/{zoneId}",  response_model=ZoneInfoResponse, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs/{dggrsId}/zones/{zoneId}", response_model=ZoneInfoResponse, tags=['ogc-dggs-api'])
async def dggrs_zone_info(req: Request, zoneinfoReq: ZoneInfoRequest = Depends(),
                          dggrs_descrption: DggrsDescription = Depends(_get_dggrs_description),
                          dggrs_provider: AbstractDGGRSProvider = Depends(_get_dggrs_provider),
                          collection: Dict[str, Collection] = Depends(_get_collection),
                          collection_provider: Dict[str, AbstractCollectionProvider] = Depends(_get_collection_provider)):
    try:
        info = query_zone_info(zoneinfoReq, req.url, dggrs_descrption, dggrs_provider, collection, collection_provider)
    except Exception as e:
        logging.error(f'{__name__} query zone info fail: {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} query zone info fail: {e}')
    if (info is None):
        return Response(status_code=204)
    return info


# Zone query conformance class

@router.get("/dggs/{dggrsId}/zones", response_model=Union[ZonesResponse, ZonesGeoJson], tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs/{dggrsId}/zones", response_model=Union[ZonesResponse, ZonesGeoJson], tags=['ogc-dggs-api'])
async def list_dggrs_zones(req: Request, zonesReq: ZonesRequest = Depends(),
                           dggrs_description: DggrsDescription = Depends(_get_dggrs_description),
                           dggrs_provider: AbstractDGGRSProvider = Depends(_get_dggrs_provider),
                           collection: Dict[str, Collection] = Depends(_get_collection),
                           collection_provider: Dict[str, AbstractCollectionProvider] = Depends(_get_collection_provider)):

    returntype = _get_return_type(req, zone_query_support_returntype, 'application/json')
    returngeometry = zonesReq.geometry if (zonesReq.geometry is not None) else 'zone-region'
    zone_level = zonesReq.zone_level if (zonesReq.zone_level is not None) else dggrs_description.defaultDepth
    compact_zone = zonesReq.compact_zone if (zonesReq.compact_zone is not None) else True
    limit = zonesReq.limit if (zonesReq.limit is not None) else 100000
    parent_zone = zonesReq.parent_zone
    bbox = zonesReq.bbox
    # Parameters checking
    if (parent_zone is not None):
        parent_level = dggrs_provider.get_cells_zone_level([parent_zone])[0]
        if (parent_level > zone_level):
            logging.error(f'{__name__} query zones list, parent level({parent_level}) > zone level({zone_level})')
            raise HTTPException(status_code=400, detail=f"query zones list, parent level({parent_level}) > zone level({zone_level})")
    for k, v in collection.items():
        max_ = v.collection_provider.maxzonelevel
        # if the dggrsId is not the primary dggrs supported by the collection.
        if (zonesReq.dggrsId != v.collection_provider.dggrsId):
            max_ = v.collection_provider.maxzonelevel + dggrs_provider.dggrs_conversion[v.collection_provider.dggrsId].zonelevel_offset
        if (zone_level > max_):
            logging.error(f'{__name__} query zones list, zone level {zone_level} > {max_}')
            raise HTTPException(status_code=400, detail=f"{__name__} query zones list, zone level {zone_level} > {max_}")
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
            raise HTTPException(status_code=400, detail=f"{__name__} query zones list, bbox converstion failed : {e}")
    try:
        result = query_zones_list(bbox, zone_level, limit, dggrs_description, dggrs_provider, collection, collection_provider,
                                  compact_zone, zonesReq.parent_zone, returntype, returngeometry)
        if (result is None):
            return Response(status_code=204)
        return result
    except Exception as e:
        logging.error(f'{__name__} query zones list failed: {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} query zones list failed: {e}')

# Data-retrieval conformance class


@router.get("/dggs/{dggrsId}/zones/{zoneId}/data", response_model=None, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs/{dggrsId}/zones/{zoneId}/data", response_model=None, tags=['ogc-dggs-api'])
async def dggrs_zones_data(req: Request, zonedataReq: ZonesDataRequest = Depends(),
                           dggrs_description: DggrsDescription = Depends(_get_dggrs_description),
                           dggrs_provider: AbstractDGGRSProvider = Depends(_get_dggrs_provider),
                           collection: Dict[str, Collection] = Depends(_get_collection),
                           collection_provider: Dict[str, AbstractCollectionProvider] = Depends(_get_collection_provider)) -> ZonesDataDggsJsonResponse | FileResponse:
    returntype = _get_return_type(req, support_returntype, 'application/json')
    zoneId = zonedataReq.zoneId
    depth = zonedataReq.depth
    returngeometry = zonedataReq.geometry if (zonedataReq.geometry is not None) else 'zone-region'
    zone_level = []
    exclude = True if depth is not None else False
    # prepare zone levels from zoneId + depth
    # The first element of zone_level will be the zoneId's level, follow by the required relative depth (zoneId's level + d)
    try:
        zone_level.append(dggrs_provider.get_cells_zone_level([zoneId])[0])
    except Exception as e:
        logging.error(f'{__name__} query zone data {zonedataReq.dggrsId}, zone id {zoneId} get zone level error: {e}')
        raise HTTPException(status_code=500, detail=f'{__name__} query zone data {zonedataReq.dggrsId}, zone id {zoneId} get zone level error: {e}')
    if (depth is not None):
        if (len(depth) == 2):
            exclude = False if depth[0] == 0 else exclude
            depth = list(range(depth[0], depth[1] + 1))
        zone_level = zone_level + [zone_level[0] + d for d in depth if d > 0]
    for k, v in collection.items():
        max_ = v.collection_provider.maxzonelevel
        # if the dggrsId is not the primary dggrs supported by the collection.
        if (zonedataReq.dggrsId != v.collection_provider.dggrsId):
            max_ = v.collection_provider.maxzonelevel + dggrs_provider.dggrs_conversion[v.collection_provider.dggrsId].zonelevel_offset
        for z in zone_level:
            if (z > max_):
                logging.error(f'{__name__} query zone data {zonedataReq.dggrsId}, zone id {zoneId} with relative depth: {z} not supported')
                raise HTTPException(status_code=400,
                                    detail=f"query zone data {zonedataReq.dggrsId}, zone id {zoneId} with relative depth: {z} not supported")
    try:
        result = query_zone_data(zoneId, zone_level, dggrs_description,
                                 dggrs_provider, collection, collection_providers, returntype, returngeometry, exclude)
        if (result is None):
            return Response(status_code=204)
        return result
    except Exception as e:
        logging.error(f'{__name__} data_retrieval failed: {e}')
        raise HTTPException(status_code=400, detail=f'{__name__} data_retrieval failed: {e}')



