from __future__ import annotations
from pydantic import BaseModel
from typing import List, Any, Dict
from typing_extensions import Self


class CollectionProvider(BaseModel):
    classname: str
    initial_params: Dict[str, Any]


class CollectionProviderGetDataReturn(BaseModel):
    zoneIds: List[str] | List[int]
    cols_meta: Dict[str, str]
    data: List[List[Any]]


# the key represents the column name and the value represents the data type of the column
class CollectionProviderGetDataDictReturn(BaseModel):
    data: Dict[str, str]
