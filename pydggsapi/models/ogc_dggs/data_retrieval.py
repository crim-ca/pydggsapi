from pydantic import ValidationError
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Link, LinkTemplate
from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataRequest, Property, Value, ZonesDataDggsJsonResponse, Feature, ZonesDataGeoJson
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint

from pydggsapi.dependencies.dggrs_providers.AbstractDGGRS import AbstractDGGRS

from fastapi.exceptions import HTTPException
from fastapi.responses import FileResponse
from clickhouse_driver import Client
from pys2index import S2PointIndex
from numcodecs import Blosc
from pprint import pprint
import shapely
import tempfile
import numpy as np
import zarr
import geopandas as gpd
import os
import logging
logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


def query_zone_data(zoneId, zone_level, depth, dggrsId, dggrslink, dggrid: AbstractDGGRS,
                    client: Client, returntype='application/dggs-json', returngeometry='zone-region'):
    logging.info(f'{__name__} query zone data {dggrsId}, zone id: {zoneId}, depth: {depth}, return: {returntype}, geometry: {returngeometry}')




