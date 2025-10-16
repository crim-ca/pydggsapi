from __future__ import annotations
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import CrsModel, Feature
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescriptionRequest
from typing import Annotated, List, Optional, Union, Tuple
from fastapi import Depends
from fastapi.exceptions import HTTPException

from pygeofilter.ast import AstType
from pygeofilter.parsers.cql_json import parse as cql_json_parser
from pygeofilter.parsers.ecql import parse as cql_text_parser
from datetime import date
from pydantic import BaseModel, conint, model_validator

import json

zone_query_support_returntype = ['application/json', 'application/geo+json']
zone_query_support_geometry = ['zone-centroid', 'zone-region']
zone_datetime_placeholder = '_pydggs_datetime'


def bbox_converter(bbox: Optional[str] = None) -> Optional[List[float]]:
    if not bbox:
        return None
    if isinstance(bbox, str):
        bbox = bbox.split(",")
    return [float(i) for i in bbox]


class ZonesRequest(DggrsDescriptionRequest):
    zone_level: Optional[conint(ge=0)] = None
    compact_zone: Optional[bool] = None
    parent_zone: Optional[Union[int, str]] = None
    limit: Optional[int] = None
    bbox_crs: Optional[str] = None
    bbox: Annotated[
        Optional[Union[List[float], Tuple[float, float, float, float]]],
        Depends(bbox_converter)
    ] = None
    geometry: Optional[str] = None
    filter: Optional[str] = None
    datetime: Optional[str] = None

    @model_validator(mode="after")
    def validation(self):
        if (self.bbox is None and self.parent_zone is None):
            raise HTTPException(status_code=400, detail='Either bbox or parent-zone must be set')
        if (self.bbox is not None):
            if (len(self.bbox) != 4):
                raise HTTPException(status_code=400, detail='bbox length is not equal to 4')
        if (self.geometry is not None):
            if (self.geometry not in zone_query_support_geometry):
                raise HTTPException(status_code=400, detail=f"{self.geometry} is not supported")
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


class ZonesResponse(BaseModel):
    zones: List[str]
    returnedAreaMetersSquare: Optional[float]


class ZonesGeoJson(BaseModel):
    type: str
    features: List[Feature]
