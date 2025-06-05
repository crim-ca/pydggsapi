from __future__ import annotations
from pydantic import BaseModel, model_validator
from typing import List, Any, Dict, Union
from typing_extensions import Self

from pydggsapi.schemas.common_geojson import GeoJSONPoint, GeoJSONPolygon


class DGGRSProviderZoneInfoReturn(BaseModel):
    zone_level: int
    shapeType: str
    centroids: List[GeoJSONPoint]
    geometry: List[GeoJSONPolygon]
    bbox: List[List[float]]
    areaMetersSquare: float


class DGGRSProviderZonesListReturn(BaseModel):
    geometry: List[GeoJSONPolygon] | List[GeoJSONPoint]
    zones: List[str] | List[int]
    returnedAreaMetersSquare: float


class DGGRSProviderZonesElement(BaseModel):
    zoneIds: List[Any]
    geometry: List[GeoJSONPolygon] | List[GeoJSONPoint]

    @model_validator(mode='after')
    def validator(self) -> Self:
        if (len(self.zoneIds) != len(self.geometry)):
            raise ValueError('length of zoneIds and geometry must equal.')
        return self


class DGGRSProviderGetRelativeZoneLevelsReturn(BaseModel):
    relative_zonelevels: Union[Dict[int, DGGRSProviderZonesElement], Dict]


class DGGRSProviderConversionReturn(BaseModel):
    zoneIds: List[Any]
    target_zoneIds: List[Any]
    target_res: List[int]

    @model_validator(mode='after')
    def validator(self) -> Self:
        if ((len(self.zoneIds) != len(self.target_zoneIds)) or (len(self.zoneIds) != len(self.target_res))):
            raise ValueError('length zoneIds, target zones id and res list must be equal.')
        return self


