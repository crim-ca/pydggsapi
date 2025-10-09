from pydggsapi.schemas.ogc_collections.collections import CollectionDesc
from pydantic import BaseModel


class Provider(BaseModel):
    providerId: str
    dggrsId: str
    maxzonelevel: int
    datasource_id: str


class Collection(CollectionDesc):
    collection_provider: Provider
