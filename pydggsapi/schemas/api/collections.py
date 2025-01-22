from __future__ import annotations
from pydantic import BaseModel
from typing import List, Dict, Any


class Provider(BaseModel):
    providerId: str
    dggrsId: str
    maxzonelevel: int
    getdata_params: Dict[str, Any]


class Collection(BaseModel):
    collectionid: str
    title: str | None
    description: str | None
    collection_provider: Provider


