from __future__ import annotations
from pydantic import BaseModel
from typing import List, Any, Dict


class CollectionProviderGetDataReturn(BaseModel):
    zoneIds: List[str] | List[int]
    cols_meta: Dict[str, str]
    data: List[List[Any]]
