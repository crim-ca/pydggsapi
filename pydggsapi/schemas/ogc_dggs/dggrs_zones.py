from __future__ import annotations
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import CrsModel, Feature
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescriptionRequest
from typing import List, Optional, Union
from fastapi import Query
from fastapi.exceptions import HTTPException

from pydantic import BaseModel, conint, Field, model_validator

zone_query_support_returntype = ['application/json', 'application/geo+json']
zone_query_support_geometry = ['zone-centroid', 'zone-region']


class ZonesRequest(DggrsDescriptionRequest):
    zone_level: Optional[conint(ge=0)] = None
    compact_zone: Optional[bool] = None
    parent_zone: Optional[Union[int, str]] = None
    limit: Optional[int] = None
    bbox_crs: Optional[str] = None
    bbox: Optional[List[float]] = Field(Query(None))
    geometry: Optional[str] = None

    @model_validator(mode="after")
    def validation(self):
        if (self.bbox is None and self.parent_zone is None):
            raise HTTPException(status_code=400, detail='Either bbox or parnet must be set')
        if (self.bbox is not None):
            if (len(self.bbox) != 4):
                raise HTTPException(status_code=400, detail='bbox lenght is not equal to 4')
        if (self.geometry is not None):
            if (self.geometry not in zone_query_support_geometry):
                raise HTTPException(status_code=400, detail=f"{self.geometry} is not supported")
        return self


class ZonesResponse(BaseModel):
    zones: List[str]
    returnedAreaMetersSquare: Optional[float]


class ZonesGeoJson(BaseModel):
    type: str
    features: List[Feature]
