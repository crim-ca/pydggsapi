# here should be DGGRID related functions and methods
# DGGRID ISEA7H resolutions
from abc import ABC, abstractmethod
from typing import List, Any, Union, Annotated
from pydggsapi.schemas.api.dggsproviders import DGGRSProviderZoneInfoReturn, DGGRSProviderZonesListReturn, DGGRSProviderGetRelativeZoneLevelsReturn
from shapely.geometry import box


class AbstractDGGRS(ABC):

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

