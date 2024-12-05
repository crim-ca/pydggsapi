# here should be all OGC DGGS API related routes
# they should all live under e.g. /dggs-api/v1-pre
# that means this module export a FastAPI router that gets mounted
# in the main api.py under /dggs-api/v1-pre

from fastapi import APIRouter, HTTPException, Depends, Response, Request
from typing import Union

from pydggsapi.schemas.ogc_dggs.dggrs_list import DggrsListResponse
from pydggsapi.schemas.ogc_dggs.dggrs_model import DggrsModelRequest, DggrsModel
from pydggsapi.schemas.ogc_dggs.dggrs_zones_info import ZoneInfoRequest, ZoneInfoResponse
from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataRequest, ZonesDataDggsJsonResponse, support_returntype
from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesRequest, ZonesResponse, ZonesGeoJson, zone_query_support_returntype
from pydggsapi.schemas.ogc_dggs.collections_dggs import Collection

from pydggsapi.models.ogc_dggs.core import query_support_dggs, query_dggrs_model, query_zone_info, landingpage, _ISEA7H_zoomlevel_fromzoneId
from pydggsapi.models.ogc_dggs.data_retrieval import query_zone_data
from pydggsapi.models.ogc_dggs.zone_query import query_zones_list

from pydggsapi.dependencies.db import get_database_client, get_conformance_classes
from pydggsapi.dependencies.dggs_isea7h import DggridISEA7H
from pydggsapi.dependencies.collections import get_collections_info
from pydggsapi.dependencies.dggrs_indexes import get_dggrs_indexes

from fastapi.responses import JSONResponse, FileResponse
import logging
import pyproj
from shapely.geometry import box
from shapely.ops import transform


logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
router = APIRouter()


def _get_return_type(req: Request, support_returntype, default_return='application/json'):
    returntypes = req.headers.get('accept').lower() if (req.headers.get('accept') is not None) else default_return
    returntypes = returntypes.split(',')
    intersection = [i for i in returntypes if i in set(support_returntype)]
    returntype = intersection[0] if (len(intersection) > 0) else default_return
    return returntype


# Landing page and conformance


@router.get("/", tags=['ogc-dggs-api'])
async def landing_page(req: Request):
    return landingpage(req.url)


@router.get("/conformance", tags=['ogc-dggs-api'])
async def conformance(conformance_classes=Depends(get_conformance_classes)):
    return JSONResponse(content={'conformsTo': conformance_classes})


# Core conformance class


@router.get("/dggs", response_model=DggrsListResponse, tags=['ogc-dggs-api'])
@router.get("/collections/{collectionId}/dggs", response_model=DggrsListResponse, tags=['ogc-dggs-api'])
async def support_dggs(req: Request, collectionId: str,
                       collections=Depends(get_collections_info), dggs_info=Depends(get_dggrs_indexes)):
    logging.info(f'{__name__} called.')
    filter_ = list(dggs_info.keys())
    if (collectionId is not None):
        if (collectionId not in list(collections.keys())):
            logging.error(f'{__name__} collection dggs: {collectionId} not found')
            raise HTTPException(status_code=500, detail=f'{__name__} collection dggs: {collectionId} not found')
        filter_ = [dggrs.dggrsId for dggrs in collections[collectionId]['dggrs_indexes']]
    return query_support_dggs(req.url, dggs_info, filter_)


@router.get("/collections/{collectionId}/dggs/{dggrs_id}", response_model=DggrsModel, tags=['ogc-dggs-api'])
@router.get("/dggs/{dggrs_id}", response_model=DggrsModel, tags=['ogc-dggs-api'])
async def dggrs_model(req: Request, dggrs_req: DggrsModelRequest = Depends(),
                      collections=Depends(get_collections_info), dggs_info=Depends(get_dggs_info)):
    current_url = req.url
    if (dggrs_req.collectionId is not None):
        if (dggrs_req.collectionId not in list(collections.keys())):
            logging.error(f'{__name__} collection dggs: {dggrs_req.collectionId} not found')
            raise HTTPException(status_code=500, detail=f'{__name__} collection dggs: {dggrs_req.collectionId} not found')
        current_url = '/'.join(str(req.url).split('/')[:-4])
    return query_dggrs_model(dggrs_req, current_url, dggs_info[dggrs_req.dggrs_id])


@router.get("/dggs/{dggrs_id}/zones/{zoneId}",  response_model=ZoneInfoResponse, tags=['ogc-dggs-api'])
async def dggrs_zone_info(req: Request, zoneinfoReq: ZoneInfoRequest = Depends(),
                          dggrid=Depends(DggridISEA7H), dggs_info=Depends(get_dggs_info)):
    return query_zone_info(zoneinfoReq, req.url, dggs_info[zoneinfoReq.dggrs_id], dggrid)

# Zone query conformance class


@router.get("/dggs/{dggrs_id}/zones", response_model=ZonesResponse | ZonesGeoJson, tags=['ogc-dggs-api'])
async def list_dggrs_zones(req: Request, zonesReq: ZonesRequest = Depends(),
                           dggrid=Depends(DggridISEA7H), dggs_info=Depends(get_dggs_info)):
    returntype = _get_return_type(req, zone_query_support_returntype, 'application/json')
    returngeometry = zonesReq.geometry if (zonesReq.geometry is not None) else 'zone-region'
    dggs_info = dggs_info[zonesReq.dggrs_id]
    zoom_level = zonesReq.zone_level if (zonesReq.zone_level is not None) else dggs_info['defaultDepth']
    compact_zone = zonesReq.compact_zone if (zonesReq.compact_zone is not None) else True
    limit = zonesReq.limit if (zonesReq.limit is not None) else 100000
    bbox = box(*zonesReq.bbox)
    bbox_crs = zonesReq.bbox_crs if (zonesReq.bbox_crs is not None) else "wgs84"
    if (bbox_crs != 'wgs84'):
        logging.info(f'{__name__} query zones list {zonesReq.dggrs_id}, original bbox: {bbox}')
        project = pyproj.Transformer.from_crs(bbox_crs, "wgs84", always_xy=True).transform
        bbox = transform(project, bbox)
        logging.info(f'{__name__} query zones list {zonesReq.dggrs_id}, transformed bbox: {bbox}')
    return query_zones_list(zonesReq.dggrs_id, bbox, zoom_level, limit, dggrid, compact_zone, zonesReq.parent_zone, returntype, returngeometry)


# Data-retrieval conformance class


@router.get("/dggs/{dggrs_id}/zones/{zoneId}/data", response_model=None, tags=['ogc-dggs-api'])
async def dggrs_zones_data(req: Request, zonedataReq: ZonesDataRequest = Depends(),
                           dggrid=Depends(DggridISEA7H), dggs_info=Depends(get_dggs_info),
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
        zone_level = [_ISEA7H_zoomlevel_fromzoneId(zoneId, dggrid)]
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


