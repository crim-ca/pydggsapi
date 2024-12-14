from __future__ import annotations
from pydantic import BaseModel
from typing import List

from pydggsapi.schemas.common_geojson import GeoJSONPoint, GeoJSONPolygon


class DGGRSProviderZoneInfoReturn(BaseModel):
    zone_level: int
    shapeType: str
    centroids: List[GeoJSONPoint]
    geometry: List[GeoJSONPolygon]
    bbox: List[List[float]]
    areaMetersSquare: float


class DGGRSProviderZonesListReturn(BaseModel):
    geometry: List[GeoJSONPolygon | GeoJSONPoint]
    zones: List[str | int]
    returnedAreaMetersSquare: float

