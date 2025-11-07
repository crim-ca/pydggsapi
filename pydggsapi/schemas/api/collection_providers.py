from __future__ import annotations
from pydantic import BaseModel
from typing import List, Any, Dict
from typing_extensions import Self

from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import Dimension


class CollectionProvider(BaseModel):
    classname: str
    datasources: Dict[str, Dict]


class CollectionProviderGetDataReturn(BaseModel):
    zoneIds: List[str] | List[int]
    cols_meta: Dict[str, str]
    data: List[List[Any]]
    dimensions: List[Dimension] | None = None


# the key represents the column name and the value represents the data type of the column
class CollectionProviderGetDataDictReturn(BaseModel):
    data: Dict[str, str]
