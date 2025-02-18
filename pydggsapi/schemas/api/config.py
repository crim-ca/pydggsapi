from __future__ import annotations
from pydantic import BaseModel
from typing import List, Dict, Any


class DataProvider(BaseModel):
    providerClassName: str
    providerParams: list[str]


class Provider(BaseModel):
    providerClassName: str
    providerParams: Dict[str, Any]


class Collection(BaseModel):
    collectionid: str
    dggrs_indexes: Dict[str, List[int]]
    title: str | None
    description: str | None
    provider: Provider

