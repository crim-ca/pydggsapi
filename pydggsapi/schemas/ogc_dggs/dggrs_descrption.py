from __future__ import annotations
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Link, LinkTemplate, CrsModel
from typing import List, Literal, Optional, Union, get_args
from pydantic import AnyHttpUrl, AnyUrl, BaseModel, Field, conint, model_validator
from fastapi import HTTPException

DgghZoneType = Literal[
    "triangle",
    "square",
    "hexagon",
    "pentagon",
    "rhombus"
]
DgghRefinementStrategy = Literal[
    "centredChildCell",
    "nestedChildCell",
    "nodeCentredChildCell",
    "nodeSharingChildCell",
    "edgeCentredChildCell",
    "faceCentredChildCell",
    "solidCentredChildCell"
]
ZirType = Literal[
    "hierarchicalConcatenation",
    "ogc2DTMSHexLevelRowCol",
    "levelRootFaceHexRowMajorSubZone"
]
SubZoneOrderType = Literal[
    "scanline",
    "spiralFromCenter",
    "mortonCurve",
    "hilbertCurve"
]


class DggrsDescriptionRequest(BaseModel):
    dggrsId: str  # = Path(...)
    collectionId: Optional[str] = None


class DgghConstraints(BaseModel):
    cellAxisAligned: Optional[bool] = Field(
        default=False,
        description=(
            "Set to true if all edges of the geometry of all zones are aligned with one of the axis of the 'crs'."
        ),
    ),
    cellConformal: Optional[bool] = Field(default=False)
    cellEquiAngular: Optional[bool] = Field(default=False)
    cellEquiDistant: Optional[bool] = Field(default=False)
    cellEqualSized: Optional[bool] = Field(
        default=False,
        description=(
            "Set to true if the area of all zones is the same for a particular zone geometry type of any "
            "specific discrete global grid of the DGG hierarchy."
        ),
    )


class DgghOrientation(BaseModel):
    # use DGGRID defaults for backward compatibility of reported values if not overridden
    # otherwise, it is STRONGLY recommended to override with reference DGGRS definitions
    # see: https://docs.ogc.org/DRAFTS/21-038r1.html#annex-dggrs-def
    latitude: Union[float, int] = Field(
        default=58.28252559,
        description="Reference geodetic Latitude in decimal degrees to fix the orientation of the polyhedron.",
        examples=[58.397145907431],
    )
    longitude: Union[float, int] = Field(
        default=11.25,
        description="Reference Longitude in decimal degrees to fix the orientation of the first vertex.",
        examples=[11.20],
    )
    azimuth: Optional[Union[float, int]] = Field(
        default=0.0,
        description="Azimuth in decimal degrees of second vertex relative to the first vertex.",
        examples=[0.0],
    )


class DgghParameters(BaseModel):
    ellipsoid: Optional[Union[AnyHttpUrl, str]] = Field(
        default=0.0,
        description="Globe Reference System Identifier/Specification",
        examples=["EPGS:7019"],
    )
    orientation: Optional[DgghOrientation] = None


class DgghDefinition(BaseModel):
    crs: Optional[CrsModel] = Field(
        default=None,
        description=(
            "The native Coordinate Reference System (CRS) in which the geometry of "
            "the zones for this DGG hierarchy is defined."
        )
    )
    basePolyhedron: Optional[str] = Field(
        default=None,
        description=(
            "The Type/Class of Polyhedron used to construct the Discrete Global Grid System "
            "if it is constructed using a Base Polyhedron. "
        ),
        examples=["icosahedron"],

    )
    refinementRatio: Optional[int] = Field(
        default=None,
        description=(
            "The ratio of the area of zones between two consecutive hierarchy level "
            "(the ratio of child zones to parent zones, also called the aperture)."
        ),
        examples=[9],
    )
    constraints: Optional[DgghConstraints] = None
    spatialDimensions: int = Field(
        default=2,  # DGGRS that are supported in this API are all 2D coordinates
        description=(
            "Number of Spatial Dimensions defined by the Discrete Global Grid System. "
            "This represents the number of spatial dimensions embedded within respective zone IDs, "
            "to locate the specific zone. Typically, this would be 2 or 3 based on 2D or 3D coordinate systems, "
            "but can represent other encoding strategies."
        ),
    )
    # see: https://github.com/opengeospatial/ogcapi-discrete-global-grid-systems/issues/102
    # see: https://docs.ogc.org/as/20-040r3/20-040r3.html#toc20
    temporalDimensions: int = Field(
        default=0,  # DGGRS that are supported in this API are all 0-dims temporal
        description=(
            "Number of Temporal Dimensions defined by the Discrete Global Grid System. "
            "This represents temporal reference system natively embedded within respective zone IDs, "
            "and not a DGGRS with temporal awareness accessed by other means than the zone IDs themselves "
            "(e.g.: using 'datetime' query, CQL2, or another property). "
        ),
    )
    zoneTypes: Optional[List[Union[DgghZoneType, str]]] = Field(
        examples=list(get_args(DgghZoneType)),
    )
    refinementStrategy: Optional[List[DgghRefinementStrategy]] = Field(
        description="The refinement strategy used by the Discrete Global Grid System",
        examples=list(get_args(DgghRefinementStrategy)),
    )
    parameters: Optional[DgghParameters] = Field(
        default=None,
        description=(
            "The optional parameters establishing a very specific Discrete Global Grid System, "
            "where each zone has a well-defined geometry."
        ),
    )


class DggrsHierarchy(BaseModel):
    """
    The hierarchical series of Discrete Global Grid upon which this DGGRS is based, including any parameters.
    """
    # see: https://docs.ogc.org/DRAFTS/21-038r1.html#annex-dggrs-def

    definition: DgghDefinition


class ZirDefinition(BaseModel):
    description: str = Field(
        description="Detailed human-readable description of the ZIR representation."
    )
    type: Optional[Union[str, ZirType]] = Field(
        default=None,
        examples=list(get_args(ZirType)),
    )


class DggrsZirs(BaseModel):
    textZIRS: ZirDefinition = Field(
        description="textual zone identifier indexing scheme",
    )
    uint64ZIRS: ZirDefinition = Field(
        default="ogc2DTMSHexLevelRowCol",
        description="64-bit unsigned integer zone indexing scheme",
    )


class DggrsSubZoneOrder(BaseModel):
    description: str = Field(
        description="Detailed human-readable description of the sub-zone ordering method."
    )
    type: Optional[Union[str, SubZoneOrderType]] = Field(
        default=None,
        examples=list(get_args(SubZoneOrderType)),
    )


class DggrsDefinition(BaseModel):
    dggh: Optional[DggrsHierarchy] = Field(
        default=None,
        description="The hierarchical series of Discrete Global Grid upon which this DGGRS is based, including any parameters.",
    )
    zirs: Optional[DggrsZirs] = Field(
        default=None,
        description="The Zone Identifier Reference System used for this Discrete Global Grid System Reference System.",
    )
    subZoneOrder: Optional[DggrsSubZoneOrder] = Field(
        default=None,
        description=(
            "The ordering used for this Discrete Global Grid System Reference System when encoding "
            "the values associated with sub-zones at any given depth relative to a parent zone."
        ),
    )


class DggrsDescription(BaseModel):
    id: str = Field(
        ...,
        description='Local DGGRS identifier consistent with the `{dggrsId}` parameter of `/dggs/{dggrsId}` resources.',
    )
    title: str = Field(
        ...,
        description='Title of this Discrete Global Grid Reference System, intended for displaying to a human',
    )
    description: str = Field(
        ...,
        description='Brief narrative description of this Discrete Global Grid System, normally available for display to a human',
    )
    keywords: Optional[List[str]] = Field(
        None,
        description='Unordered list of one or more commonly used or formalized word(s) or phrase(s) used to describe this Discrete Global Grid Reference System',
    )
    uri: Optional[AnyUrl] = Field(
        None,
        description='Identifier for this Discrete Global Grid Reference System registered with an authority.',
    )
    crs: Optional[CrsModel] = None
    defaultDepth: Union[conint(ge=0), str] = Field(
        ...,
        description='The default zone depth returned for zone data retrieval when the `zone-depth` parameter is not used. This is the DGGS resolution levels beyond the requested DGGS zone’s hierarchy level included in the response, when retrieving data for a particular zone. This can be either: • A single positive integer value — representing a specific zone depth to return e.g., `5`; • A range of positive integer values in the form “{low}-{high}” — representing a\n  continuous range of zone depths to return e.g., `1-8`; or,\n• A comma separated list of at least two (2) positive integer values — representing a\n  set of specific zone depths to return e.g., `1,3,7`.\n  A particular data encoding imply a particular zone depth and not support the default zone depth specified here,\n  in which case the default zone depth (or the only possible depth) for that encoding will be used.',
    )
    maxRefinementLevel: Optional[conint(ge=0)] = Field(
        None,
        description='The maximum refinement level at which the full resolution of the data can be retrieved for this DGGRS and origin (using a `zone-depth` relative depth of 0) and/or used for performing the most accurate zone queries (using that value for `zone-level`)',
    )
    maxRelativeDepth: Optional[conint(ge=0)] = Field(
        None,
        description='The maximum relative depth at which the full resolution of the data can be retrieved for this DGGRS and origin',
    )
    links: List[Link] = Field(
        ...,
        description='Links to related resources. A `self` link to the Discrete Global Grid Reference System description and an `[ogc-rel:dggrs-definition]` link to the DGGRS definition (using the schema defined by https://github.com/opengeospatial/ogcapi-discrete-global-grid-systems/blob/master/core/schemas/dggrs-definition/dggrs-definition-proposed.yaml) are required. An `[ogc-rel:dggrs-zone-query]` link to query DGGS zones should also be included if _DGGS Zone Query_ is supported.',
    )
    linkTemplates: Optional[List[LinkTemplate]] = Field(
        None,
        description='Templated Links to related resources. A templated `[ogc-rel:dggrs-zone-data]` link to retrieve data should be included if _DGGS Zone Data_ is supported.',
    )
