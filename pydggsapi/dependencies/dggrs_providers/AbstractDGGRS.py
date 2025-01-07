# here should be DGGRID related functions and methods
# DGGRID ISEA7H resolutions
from abc import ABC, abstractmethod
from typing import List, Any, Union, Tuple
from pydggsapi.schemas.api.dggsproviders import DGGRSProviderZoneInfoReturn, DGGRSProviderZonesListReturn, DGGRSProviderGetRelativeZoneLevelsReturn
from pydggsapi.schemas.api.dggsproviders import VirtualAbstractDGGRSForwardReturn
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


class VirtualAbstractDGGRS(AbstractDGGRS):

    def __init__(self, virtual, actual):
        self.virtualdggrs = virtual
        self.actualdggrs = actual

    # convert virtual zones id to actual zones id.
    # It return a tuple of list, the first one is the zone id and the second one is zone level
    # 1. It should return actual zone id at finer level, ie. the number of unique actual zone id return >=  unique virtual one.
    @abstractmethod
    def convert(self, virtual_zoneIds: list) -> VirtualAbstractDGGRSForwardReturn:
        raise NotImplementedError
