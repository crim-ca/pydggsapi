from __future__ import annotations
from pydantic import BaseModel
from typing import List, Dict, Any


class CollectionDggrsInfo(BaseModel):
    dggrsId: str
    zoom_level: List[int]


class Provider(BaseModel):
    providerClassName: str
    providerParams: Dict[str, Any]


class Collection(BaseModel):
    collectionid: str
    dggrs_indexes: List[CollectionDggrsInfo]
    title: str | None
    description: str | None
    provider: Provider

