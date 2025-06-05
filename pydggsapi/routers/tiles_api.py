# here we should separate the routes that are only related to the MVT
# visualisation, the tiles.json, and the actual /x/y/z routes
# I suggest to bundle these under /tiles/ or /tiles-api/ (doesn't need a version, because standard)
from fastapi import APIRouter, Body, HTTPException, Depends, Response, Request
from typing import Annotated


from pydggsapi.schemas.tiles.tiles import TilesRequest
from pydggsapi.schemas.ogc_dggs.dggrs_zones import ZonesRequest, ZonesResponse
from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataRequest

from pydggsapi.dependencies.api.mercator import Mercator
from pydggsapi.routers.dggs_api import _get_collection, _get_dggrs_provider, list_dggrs_zones, dggrs_zones_data, _get_dggrs_description
from pydggsapi.routers.dggs_api import _get_collection_provider

from starlette.datastructures import MutableHeaders
import asyncio
import nest_asyncio
import pyproj
import json
import shapely
from shapely.geometry import box, shape
from shapely.ops import transform
import mapbox_vector_tile
import logging
logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
nest_asyncio.apply()

router = APIRouter()


SRID_LNGLAT = 4326
SRID_SPHERICAL_MERCATOR = 3857
project = pyproj.Transformer.from_crs(SRID_SPHERICAL_MERCATOR, SRID_LNGLAT, always_xy=True).transform
transformer = pyproj.Transformer.from_crs(crs_from=SRID_LNGLAT, crs_to=SRID_SPHERICAL_MERCATOR, always_xy=True)


@router.get("/{collectionId}/{z}/{x}/{y}", tags=['tiles-api'])
@router.get("/{collectionId}/{dggrsId}/{z}/{x}/{y}", tags=['tiles-api'])
async def query_mvt_tiles(req: Request, tilesreq: TilesRequest = Depends(),
                          mercator=Depends(Mercator)):
    logging.info(f'{__name__} tiles info: {tilesreq.collectionId} {tilesreq.dggrsId} {tilesreq.z} {tilesreq.x} {tilesreq.y}')
    collection_info = _get_collection(tilesreq.collectionId, tilesreq.dggrsId)
    if (tilesreq.dggrsId is None):
        tilesreq.dggrsId = collection_info[tilesreq.collectionId].collection_provider.dggrsId
    dggrs = _get_dggrs_provider(tilesreq.dggrsId)

    bbox, tile = mercator.getWGS84bbox(tilesreq.z, tilesreq.x, tilesreq.y)
    res_info = mercator.get(tile.z)
    tile_width_km = float(res_info["Tile width deg lons"]) / 0.01 * 0.4  # in tile_width_km
    zone_level = dggrs.get_zone_level_by_cls(tile_width_km)
    clip_bound = box(bbox.left, bbox.bottom, bbox.right, bbox.top)
    clip_bound = shapely.total_bounds(transform(project, clip_bound))
    if zone_level > collection_info[tilesreq.collectionId].collection_provider.maxzonelevel:
        zone_level = collection_info[tilesreq.collectionId].collection_provider.maxzonelevel

    logging.info(f'{__name__} zone level:{zone_level}, tile width:{tile_width_km}, bbox:{bbox}')
    zonesReq = ZonesRequest(collectionId=tilesreq.collectionId, dggrsId=tilesreq.dggrsId, zone_level=zone_level,
                            compact_zone=False, bbox=clip_bound)
    collection_provider = _get_collection_provider(collection_info[tilesreq.collectionId].collection_provider.providerId)

    zones_id_response = await list_dggrs_zones(req, zonesReq, _get_dggrs_description(tilesreq.dggrsId), dggrs,
                                               collection_info, collection_provider)
    if (type(zones_id_response) is Response):
        content = mapbox_vector_tile.encode({"name": 'weighted_suitability', "features": []},
                                            quantize_bounds=bbox,
                                            default_options={"transformer": transformer.transform})
        return Response(bytes(content), media_type="application/x-protobuf")
    logging.info(f'{__name__} zones id list length: {len(zones_id_response.zones)}')
    new_header = MutableHeaders(req._headers)
    new_header['accept'] = 'application/geo+json'
    req._headers = new_header
    req.scope.update(headers=req.headers.raw)
    features = []
    tasks = []
    loop = asyncio.get_event_loop()
    for zoneid in zones_id_response.zones:
        zonedatareq = ZonesDataRequest(zoneId=zoneid, dggrsId=tilesreq.dggrsId, collectionId=tilesreq.collectionId)
        tasks.append(dggrs_zones_data(req, zonedatareq,  _get_dggrs_description(tilesreq.dggrsId),
                     dggrs, collection_info, collection_provider))
    tasks = loop.run_until_complete(asyncio.gather(*tasks))
    #loop.close()
    features = [{'geometry': shapely.from_geojson(json.dumps(i.geometry.__dict__)), 'properties': i.properties}
                for t in tasks if (type(t) is not Response) for i in t.features]
    content = mapbox_vector_tile.encode({"name": tilesreq.collectionId, "features": features},
                                        quantize_bounds=bbox,
                                        default_options={"transformer": transformer.transform})
    return Response(bytes(content), media_type="application/x-protobuf")


#@router.get("/{layer}.json", tags=['tiles-api'])
#async def get_layer_json(request: Request, layer: str, client=Depends(get_database_client)):
#    logging.info(f'{__name__} suitability get_layer_json called')
#    return get_tiles_json(client, request.url, layer)

