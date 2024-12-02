# here we should separate the routes that are only related to the MVT
# visualisation, the tiles.json, and the actual /x/y/z routes
# I suggest to bundle these under /tiles/ or /tiles-api/ (doesn't need a version, because standard)
from fastapi import APIRouter, Body, HTTPException, Depends, Response, Request
from typing import Annotated

from pydggsapi.models.tiles_model import get_suitability_features, get_tiles_json, transformer
from pydggsapi.schemas.tiles.tiles import TilesRequest
from pydggsapi.dependencies.dggs_isea7h import DggridISEA7H
from pydggsapi.dependencies.mercator import Mercator
from pydggsapi.dependencies.db import get_database_client

import mapbox_vector_tile
import logging
logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
router = APIRouter()


@router.get("/{layer}/{z}/{x}/{y}", tags=['tiles-api'])
async def get_suitability_tile(client=Depends(get_database_client),
                               param: TilesRequest = Depends(),
                               mercator=Depends(Mercator),
                               dggrid=Depends(DggridISEA7H)):
    # input checking
    # the layer params come in "(kuuid)@(weights_setting_name)@(country_encoded)" format
    logging.info(f'{__name__} suitability {param.layer} {param.z} {param.x} {param.y} called')
    tiles_features, bbox = get_suitability_features(client, param, dggrid, mercator)
    content = mapbox_vector_tile.encode({"name": 'weighted_suitability', "features": tiles_features.features},
                                        quantize_bounds=bbox,
                                        default_options={"transformer": transformer.transform})
    return Response(bytes(content), media_type="application/x-protobuf")


@router.get("/{layer}.json", tags=['tiles-api'])
async def get_layer_json(request: Request, layer: str, client=Depends(get_database_client)):
    logging.info(f'{__name__} suitability get_layer_json called')
    return get_tiles_json(client, request.url, layer)

