# here should be DGGRID related functions and methods
# DGGRID ISEA7H resolutions
from abc import ABC, abstractmethod
from typing import List, Any, Union, Tuple, Dict, Optional
from pydggsapi.schemas.api.dggrs_providers import DGGRSProviderZoneInfoReturn, DGGRSProviderZonesListReturn, DGGRSProviderGetRelativeZoneLevelsReturn
from pydggsapi.schemas.api.dggrs_providers import DGGRSProviderConversionReturn
from pydantic import BaseModel
from shapely.geometry import box


class conversion_properties(BaseModel):
    zonelevel_offset: int


class AbstractDGGRS(ABC):

    dggrs_conversion: Optional[Dict[str, conversion_properties]] = {}

    @abstractmethod
    def get_cells_zone_level(self, cellIds: list) -> List[int]:
        raise NotImplementedError

    # for each zone level, the len of zoneId list and geometry must be equal
    @abstractmethod
    def get_relative_zonelevels(self, cellId: Any, base_level: int, zone_levels: List[int], geometry: str) -> DGGRSProviderGetRelativeZoneLevelsReturn:
        raise NotImplementedError

    @abstractmethod
    def zoneslist(self, bbox: Union[box, None], zone_level: int, parent_zone: Union[str, int, None],
                  returngeometry: str, compact=True) -> DGGRSProviderZonesListReturn:
        raise NotImplementedError

    @abstractmethod
    def zonesinfo(self, cellIds: list) -> DGGRSProviderZoneInfoReturn:
        raise NotImplementedError

    @abstractmethod
    def convert(self, virtual_zoneIds: list, targetdggrs: Any) -> DGGRSProviderConversionReturn:
        raise NotImplementedError
