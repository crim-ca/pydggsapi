from __future__ import annotations
from pydantic import BaseModel, model_validator
from typing import List, Dict, Any
from fastapi.exceptions import HTTPException


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
    bounds: List[float] = []

    @model_validator(mode="after")
    def validation(self):
        if (len(self.bounds) != 4 and len(self.bounds) != 0):
            raise HTTPException(status_code=400, detail='The length of collection bounds is not equal to 4.')
        return self
