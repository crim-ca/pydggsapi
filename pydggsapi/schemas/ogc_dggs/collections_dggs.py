from __future__ import annotations
from pydantic import AnyUrl, BaseModel, Field, conint
from typing import Optional


class Collection(BaseModel):
    collectionId: Optional[str] = None
