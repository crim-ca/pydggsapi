from __future__ import annotations
from typing import List, Optional, Union
from pydantic import BaseModel, Field, model_validator, conlist
from fastapi.exceptions import HTTPException


class Spatial(BaseModel):
    bbox: List[List[float]]
    storageCrsBbox: Optional[List[float]] = None
    crs: Optional[List[str]] = Field(
        ['http://www.opengis.net/def/crs/OGC/1.3/CRS84'],
        description='the list of coordinate reference systems supported by the API; the first item is the default coordinate reference system',
        example=[
            'http://www.opengis.net/def/crs/OGC/1.3/CRS84',
            'http://www.opengis.net/def/crs/EPSG/0/4326',
        ],
    )
    grid: Optional[str] = ''

    @model_validator(mode="after")
    def validation(self):
        if (len(self.bbox) != 0):
            for b in self.bbox:
                if (len(b) != 4 and len(b) != 6):
                    raise HTTPException(status_code=400, detail='The length of collection bbox is not equal to 4 or 6.')
        return self


class Temporal(BaseModel):
    interval: Optional[conlist(str, min_length=1, max_length=2)] = None
    crs: Optional[List[str]] = Field(
        [''],
        description="""Coordinate reference system of the coordinates in the temporal extent
                         (property interval). The default reference system is the Gregorian calendar.
                         For data for which the Gregorian calendar is not suitable, such as geological time scale,
                         another temporal reference system may be used""",
        example=[
            'http://www.opengis.net/def/uom/ISO-8601/0/Gregorian'
        ],
    )
    grid: Optional[str] = ''


