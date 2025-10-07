from pydggsapi.schemas.ogc_collections.collections import CollectionDesc
from pydantic import BaseModel
from typing import Dict, Any


class Provider(BaseModel):
    providerId: str
    dggrsId: str
    maxzonelevel: int
    getdata_params: Dict[str, Any]


class Collection(CollectionDesc):
    collection_provider: Provider
