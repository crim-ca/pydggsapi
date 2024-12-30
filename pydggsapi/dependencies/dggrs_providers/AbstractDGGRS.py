# here should be DGGRID related functions and methods
# DGGRID ISEA7H resolutions
from abc import ABC, abstractmethod
from typing import List, Any, Union
from pydggsapi.schemas.api.dggsproviders import DGGRSProviderZoneInfoReturn, DGGRSProviderZonesListReturn
from shapely.geometry import box


class AbstractDGGRS(ABC):

    @abstractmethod
    def get_cells_zone_level(self, cellIds: list) -> List[int]:
        raise NotImplementedError

    @abstractmethod
    def get_zoneIds_by_zonelevels(self, cellId: Any, base_level: int, zone_levels: List[int], geometry: str) -> tuple[List[List[Any]], List[List[Any]]]:
        raise NotImplementedError

    @abstractmethod
    def zoneslist(self, bbox: Union[box, None], zone_level: int, parent_zone: Union[str, int, None],
                  returngeometry: str, compact=True) -> DGGRSProviderZonesListReturn:
        raise NotImplementedError

    @abstractmethod
    def zonesinfo(self, cellIds: list) -> DGGRSProviderZoneInfoReturn:
        raise NotImplementedError

