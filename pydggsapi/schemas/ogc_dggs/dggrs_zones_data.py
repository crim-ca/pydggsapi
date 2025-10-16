from __future__ import annotations
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import CrsModel, Feature
from pydggsapi.schemas.ogc_dggs.dggrs_zones_info import ZoneInfoRequest
from pydggsapi.schemas.ogc_dggs.dggrs_zones import zone_datetime_placeholder
from pydggsapi.schemas.common_geojson import GeoJSONPoint, GeoJSONPolygon

from pygeofilter.parsers.cql_json import parse as cql_json_parser
from pygeofilter.parsers.ecql import parse as cql_text_parser
from datetime import date
import json
from typing import List, Optional, Dict, Union, Any
from fastapi import Query
from fastapi.exceptions import HTTPException
import re
from pydantic import AnyUrl, BaseModel, Field, model_validator, ValidationError

support_returntype = ['application/json', 'application/zarr+zip', 'application/geo+json']
support_geometry = ['zone-centroid', 'zone-region']


class ZonesDataRequest(ZoneInfoRequest):
    depth: Optional[str] = None  # Field(pattern=r'', default=None)
    geometry: Optional[str] = None
    filter: Optional[str] = None
    datetime: Optional[str] = None

    @model_validator(mode='after')
    def validator(self):
        if (self.depth is not None):
            if (not re.match("(\d{1,2})|(\d{1,2}-\d{1,2})", self.depth)):
                raise HTTPException(status_code=500, detail="depth must be either a integer or in range (int-int) format")
            depth = self.depth.split("-")
            try:
                if (len(depth) == 1):
                    self.depth = [int(depth[0])]
                else:
                    if (int(depth[0]) > int(depth[1])):
                        raise HTTPException(status_code=500, detail="depth range is not in order")
                    self.depth = [int(depth[0]), int(depth[1])]
            except ValueError:
                raise HTTPException(status_code=500, detail="depth must be integer >=0 ")
        if (self.geometry is not None):
            if (self.geometry not in support_geometry):
                raise HTTPException(status_code=500, detail=f"{self.geometry} is not supported")
        if (self.datetime is not None):
            self.datetime = self.datetime.split("/")
            try:
                self.datetime = [date.fromisoformat(d) for d in self.datetime]
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f'datetime format error: {e}')
            datetime_query = f"({zone_datetime_placeholder} = {self.datetime[0]})"
            if (len(self.datetime) > 1):
                datetime_query = f"(({zone_datetime_placeholder} >= {self.datetime[0]}) and ({zone_datetime_placeholder} <= {self.datetime[2]}))"
            if (self.filter is not None):
                self.filter += f" AND {datetime_query}"
            else:
                self.filter = datetime_query
        if (self.filter is not None):
            parser = cql_text_parser
            try:
                self.filter = json.loads(self.filter)
                parser = cql_json_parser
            except ValueError:
                pass  # then it is a cql-text (or not)
            try:
                self.filter = parser(self.filter)
            except Exception as er:
                raise HTTPException(status_code=400, detail=f"{self.filter} is not a valid CQL-text or CQL-json :{er}")
        return self


class Property(BaseModel):
    type: str
    title: Optional[str] = None


class Value(BaseModel):
    depth: int
    # FIXME: invalid 'shape' object
    #   (https://github.com/opengeospatial/ogcapi-discrete-global-grid-systems/blob/ea1a2ad/core/schemas/dggs-json/dggs-json.yaml#L104)
    shape: Dict[str, int]
    data: list[Any]


class DimensionGrid(BaseModel):
    type: str
    coordinates: List[List[float]]  # List of [lon, lat] pairs


class Dimension(BaseModel):
    name: str
    # FIXME: technically 'grid' is 'required' by API,
    #   but can be problematic (https://github.com/opengeospatial/ogcapi-discrete-global-grid-systems/issues/94)
    grid: Optional[str] = None
    interval: Optional[Union[List[Optional[float]], List[Optional[str]]]] = None
    definition: Optional[str] = None
    unit: Optional[str] = None
    unitLang: Optional[str] = None


class ZonesDataDggsJsonResponse(BaseModel):
    dggrs: AnyUrl
    zoneId: str
    depths: List[int]
    # schema_: Optional[Schema] = Field(None, alias='schema')
    properties: Dict[str, Property]
    values: Dict[str, List[Value]]
    dimensions: Optional[List[Dimension]] = None


class ZonesDataGeoJson(BaseModel):
    type: str
    features: List[Feature]
