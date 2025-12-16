# here should be DGGRID related functions and methods
# DGGRID ISEA7H resolutions
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Literal, Optional, Tuple, Union
from pydggsapi.schemas.api.dggrs_providers import (
    ZoneIdRepresentationType,
    DGGRSProviderZonesElement,
    DGGRSProviderZoneInfoReturn,
    DGGRSProviderZonesListReturn,
    DGGRSProviderGetRelativeZoneLevelsReturn,
    DGGRSProviderConversionReturn
)
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import ReturnGeometryTypes
from pydantic import BaseModel
from shapely.geometry import box


class conversion_properties(BaseModel):
    zonelevel_offset: int


class AbstractDGGRSProvider(ABC):

    dggrs_conversion: Optional[Dict[str, conversion_properties]] = {}

    @abstractmethod
    def zone_id_from_textual(self, cellIds: List[str], zone_id_repr: ZoneIdRepresentationType) -> List[Any]:
        raise NotImplementedError

    @abstractmethod
    def zone_id_to_textual(self, cellIds: List[Any], zone_id_repr: ZoneIdRepresentationType, refinement_level=None) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    # return unit km
    def get_cls_by_zone_level(self, zone_level: int) -> float:
        raise NotImplementedError

    @abstractmethod
    def get_zone_level_by_cls(self, cls_km: float) -> int:
        raise NotImplementedError

    @abstractmethod
    def get_cells_zone_level(self, cellIds: List[str]) -> List[int]:
        raise NotImplementedError

    # for each zone level, the len of zoneId list and geometry must be equal
    @abstractmethod
    def get_relative_zonelevels(self, cellId: str, base_level: int, zone_levels: List[int],
                                geometry: Optional[ReturnGeometryTypes] = "zone-region") -> DGGRSProviderGetRelativeZoneLevelsReturn:
        raise NotImplementedError

    @abstractmethod
    def zoneslist(self, bbox: Union[box, None], zone_level: int, parent_zone: Union[str, int, None],
                  returngeometry: ReturnGeometryTypes, compact: bool = True) -> DGGRSProviderZonesListReturn:
        raise NotImplementedError

    @abstractmethod
    def zonesinfo(self, cellIds: List[str]) -> DGGRSProviderZoneInfoReturn:
        raise NotImplementedError

    @abstractmethod
    def convert(self, zoneIds: List[str], targetdggrs: str,
                zone_id_repr: ZoneIdRepresentationType = 'textual') -> DGGRSProviderConversionReturn:
        raise NotImplementedError
