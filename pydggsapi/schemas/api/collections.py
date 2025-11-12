from pydggsapi.schemas.ogc_collections.collections import CollectionDesc
from pydantic import BaseModel
from typing import Literal


class Provider(BaseModel):
    providerId: str
    dggrsId: str
    dggrs_zoneid_repr: Literal['int', 'textual', 'hexstring'] = 'textual'
    max_refinement_level: int
    min_refinement_level: int
    datasource_id: str


class Collection(CollectionDesc):
    collection_provider: Provider
