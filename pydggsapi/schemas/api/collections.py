from pydggsapi.schemas.ogc_collections.collections import CollectionDesc
from pydggsapi.schemas.api.dggrs_providers import ZoneIdRepresentationType
from typing import Literal
from pydantic import BaseModel


class Provider(BaseModel):
    providerId: str
    dggrsId: str
    dggrs_zoneid_repr: Literal[tuple(ZoneIdRepresentationType)] = "textual"
    max_refinement_level: int
    min_refinement_level: int
    datasource_id: str


class Collection(CollectionDesc):
    collection_provider: Provider
