from __future__ import annotations
from pydantic import AnyUrl, BaseModel, Field, conint
from typing import List, Optional


class CollectionDggrsInfo(BaseModel):
    dggs_indexes: str
    zoom_level: List[int]


class CollectionInfo(BaseModel):
    collectionid: str
    dggs_indexes: List[CollectionDggrsInfo]

