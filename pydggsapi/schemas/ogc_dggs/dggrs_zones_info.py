from __future__ import annotations
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import CrsModel, Link
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescriptionRequest
from typing import List, Optional
from pydantic import BaseModel, conint


class ZoneInfoRequest(DggrsDescriptionRequest):
    zoneId: int


class ZoneInfoResponse(BaseModel):
    id: str
    links: List[Link]
    shapeType: Optional[str]
    level: Optional[conint(ge=0)] = None
    crs: Optional[CrsModel] = None
    centroid: Optional[GeoJSONPoint] = None
    bbox: Optional[List[float]] = None
    areaMetersSquare: Optional[float] = None
    volumeMetersCube: Optional[float] = None
    temporalDurationSeconds: Optional[float] = None
    geometry: Optional[GeoJSONPolygon] = None
    temporalInterval: Optional[List[str]] = None
    # statistics: Optional[Dict[str, Statistics]]
