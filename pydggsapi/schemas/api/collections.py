from pydggsapi.schemas.ogc_collections.collections import CollectionDesc
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Dict, List, Literal, Self
import logging

logger = logging.getLogger()

# corresponds to Dataframe aggregation methods
AggregationMethodType = Literal['sum', 'mean', 'max', 'min', 'median', 'mode']
ZoneDataPropertyQuantizationMethod = Dict[str, AggregationMethodType]


class Provider(BaseModel):
    providerId: str
    dggrsId: str
    dggrs_zoneid_repr: Literal['int', 'textual', 'hexstring'] = 'textual'
    max_refinement_level: int
    min_refinement_level: int
    datasource_id: str

    data_refinement_levels: List[int] = Field(
        default_factory=list,
        description=(
            'Allows a provider to represent DGGS data using only certain intermediate refinement levels. '
            'All allowed (min/max) refinement level zones will be computed from the closest higher level and '
            'must be of lower or equal resolution (refinement levels above) to the highest available zone data. '
            'This can be used to drastically reduce the amount of stored zone data by avoiding duplication '
            'of aggregated coarser zones. Using multiple refinement levels (as intermediate "checkpoints") '
            'can help reduce the amount of zones to aggregate between refinement levels far apart.'
        ),
    )
    data_quantization_method: ZoneDataPropertyQuantizationMethod = Field(
        default_factory=dict,
        description=(
            'Mapping of property names to aggregation methods used when quantizing data from finer refinement level. '
            'If not provided for particular property, "sum" is assumed.'
        ),
    )

    def find_closest_refinement_level(self, refinement_level: int) -> int:
        if not self.data_refinement_levels:
            return refinement_level
        for level in self.data_refinement_levels:
            if level >= refinement_level:
                return level
        return self.data_refinement_levels[-1]

    @field_validator('data_refinement_levels', mode='before')
    @classmethod
    def sort_data_refinement_levels(cls, v: List[int]) -> List[int]:
        return sorted(set(v))

    @model_validator(mode='after')
    def validator(self) -> Self:
        if self.min_refinement_level > self.max_refinement_level:
            raise ValueError(
                f'{__name__} '
                'Provider min_refinement_level must be less than or equal to max_refinement_level. '
                f'Got providerId={self.providerId}, '
                f'min_refinement_level={self.min_refinement_level}, '
                f'max_refinement_level={self.max_refinement_level}.'
            )
        if self.data_refinement_levels:
            max_data_level = max(self.data_refinement_levels)
            if self.min_refinement_level > max_data_level:
                raise ValueError(
                    f'{__name__} '
                    'Provider min_refinement_level must be less or equal than at least one '
                    'of the provided data_refinement_levels. '
                    'Data cannot be provided for zones of higher refinement levels from coarser zones. '
                    f'Got providerId={self.providerId}, '
                    f'min_refinement_level={self.min_refinement_level}, '
                    f'data_refinement_level={self.data_refinement_levels}.'
                )
            if self.max_refinement_level < max_data_level:
                # technically allowed, but it would be purposely limiting available data from the API
                # this could be a valid use case (e.g.: data obfuscation, limiting response size, etc.)
                # but make sure the user is made aware of this potential misconfiguration
                logger.warning(
                    f'{__name__} '
                    'Provider max_refinement_level was set below available data_refinement_levels. '
                    f'Got providerId={self.providerId}, '
                    f'max_refinement_level={self.max_refinement_level}, '
                    f'data_refinement_level={self.data_refinement_levels}.'
                )
        return self


class Collection(CollectionDesc):
    collection_provider: Provider
