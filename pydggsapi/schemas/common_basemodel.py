from dataclasses import dataclass
from pydantic import BaseModel, ConfigDict, model_serializer


@dataclass
class _OmitIfNone:
    pass


class CommonBaseModel(BaseModel):

    @model_serializer
    def model_serialize(self):
        omit_if_none_fields = {
            key: field
            for key, field in self.model_fields.items()
            if any(isinstance(m, _OmitIfNone) for m in field.metadata)
        }
        #fields = getattr(self, "model_fields", self.__fields__)  # noqa
        values = {
            key: val
            for key, val in self
            if key not in omit_if_none_fields or val is not None
        }
        return values

    model_config = ConfigDict(
        populate_by_name=True,
    )
