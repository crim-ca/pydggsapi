from __future__ import annotations
from pydantic import BaseModel
from typing import List, Any


class CollectionProviderGetDataReturn(BaseModel):
    zoneIds: List[str] | List[int]
    cols_name: List[str]
    cols_dtype: List[str]
    data: List[List[Any]]
