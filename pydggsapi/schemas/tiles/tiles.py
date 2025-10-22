from pydantic import BaseModel, model_validator, AnyUrl, Field
from fastapi import Path
from typing import List, Optional, Dict, Any

class TilesRequest(BaseModel):
    collectionId: str = Path(...)
    dggrsId: Optional[str] = None
    z: int = Path(...,ge=0, le=25)
    x: int = Path(...)
    y: int = Path(...)
    dggrsId: str = ""
    relative_depth: int = Field(0, ge=-2, le=2)

class TilesFeatures(BaseModel):
    features: List[Dict[str,Any]]

class VectorLayer(BaseModel):
    id: str
    fields: Dict[str,str]

class TilesJSON(BaseModel):
    tilejson: str
    tiles: List[str]
    vector_layers: List[VectorLayer]
    bounds: List[List[float]]
    description: str
    name: str
