from pydggsapi.dependencies.collections_providers.abstract_collection_provider import AbstractCollectionProvider
from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import Dimension
from pydggsapi.schemas.api.collection_providers import (
    CollectionProviderGetDataDictReturn,
    CollectionProviderGetDataReturn,
)

from pystac_client import Client as STACClient
from pydantic import BaseModel, Field
from geopandas import read_file
import pandas as pd
import pystac
import shapely

from typing import List, Dict, Optional
import logging

logger = logging.getLogger()

class STAC_datasource_parameters(BaseModel):
    catalog_url: str
    collection_id: str
    zone_id_template: str = Field(
        default="{zoneId}",
        description=(
            "Indicate if a custom representation of the STAC Item ID or 'grid:code' "
            "is used to map to a DGGS Zone ID. See also 'grid_code_zone_id' parameter."
        ),
    )
    grid_code_zone_id: bool = Field(
        default=False,
        description=(
            "If enabled, look for the the DGGS Zone ID using 'grid:code' property "
            "from the 'grid' extension (https://github.com/stac-extensions/grid) instead "
            "of looking for STAC Item IDs directly. Can be used to have multiple DGGRS within "
            "a STAC Collection. The 'zone_id_template' will be applied to the 'grid:code' value."
        )
    )
    grid_reference: Optional[str] = Field(
        default=None,
        description=(
            "If specified, STAC Item search will filter only to Items containing the corresponding "
            "'grid:reference' property from the 'grid' extension (https://github.com/stac-extensions/grid)."
        ),
    )
    data_variables: List[str] = Field(
        default_factory=lambda: ["*"],
        description=(
            "List of data variables to include in this collection. Use '*' to include all variables. "
            "The property 'cube:variables' from 'datacube' extension (https://github.com/stac-extensions/datacube) "
            "is required in the STAC Collection and Items. "
        ),
    )
    exclude_data_variables: List[str] = Field(
        default_factory=list,
        description=(
            "List of data variables to exclude in this collection. "
            "The property 'cube:variables' from 'datacube' extension (https://github.com/stac-extensions/datacube) "
            "is required in the STAC Collection and Items. "
        ),
    )
    asset_variables: Dict[str, List[str]] = Field(
        default_factory=lambda: {"data": ["*"]},
        description=(
            "Defines a mapping of lookup location for variables, where keys represent the asset names "
            "and their values are the list of variables that must be extracted from them. "
            "If '*' is used, all variables from that asset are included. "
            "If there are conflicting variable names, they will be loaded "
            "in the order by which the assets are listed and returned by the STAC API, "
            "meaning that the later variables will remain. "
            "Variables not resolved from 'data_variables' and 'exclude_data_variables' will be ignored."
        ),
    )

    _client: STACClient = None


class STACCollectionProvider(AbstractCollectionProvider):
    datasources: Dict[str, STAC_datasource_parameters] = {}

    def __init__(self, params):
        try:
            filelist = params.get('datasources')
            if (filelist is not None):
                for k, v in filelist.items():
                    param = STAC_datasource_parameters(**v)
                    param._client = STACClient.open(param.catalog_url)
                    self.datasources[k] = param
        except Exception as e:
            logger.error(f'{__name__} class initial failed: {e}')
            raise Exception(f'{__name__} class initial failed: {e}')

    def get_source(self, datasource_id: str) -> Optional[STAC_datasource_parameters]:
        try:
            datasource = self.datasources[datasource_id]
        except KeyError:
            logger.error(f'{__name__} {datasource_id} not found')
            return None
        return datasource

    def get_zone_id(
        self,
        zone_id: str,
        datasource: STAC_datasource_parameters,
    ) -> str:
        return datasource.zone_id_template.format(zoneId=zone_id)

    def get_data(self, zoneIds: List[str], res: int, datasource_id: str) -> CollectionProviderGetDataReturn:
        result = CollectionProviderGetDataReturn(zoneIds=[], cols_meta={}, data=[])
        datasource = self.get_source(datasource_id)
        if not datasource:
            return result

        col_data = []
        zoneIds = {self.get_zone_id(z, datasource): z for z in zoneIds}
        zone_filter = {}
        zone_filter_ref = {"op": "eq", "args": [{"property": "grid:reference"}, datasource.grid_reference]}
        zone_filter_ids = {"op": "in", "args": [{"property": "grid:code"}, list(zoneIds)]}
        if datasource.grid_reference and datasource.grid_code_zone_id:
            zone_filter = {"op": "and", "args": [zone_filter_ref, zone_filter_ids]}
        elif datasource.grid_reference:
            zone_filter = zone_filter_ref
        elif datasource.grid_code_zone_id:
            zone_filter = zone_filter_ids
        matched_zones = []
        matched = datasource._client.search(
            collections=[datasource.collection_id],  # FIXME: support joinCollections?
            ids=list(zoneIds),
            filter=zone_filter,
            filter_lang="cql2-json" if zone_filter else None,
        )
        for item in matched.items():
            assets = item.get_assets(role="data")
            assets_data = []
            for name, asset in assets.items():
                if name not in datasource.asset_variables:
                    continue
                variables = datasource.asset_variables[name]
                var_data = read_file(asset.href)  # FIXME: maybe help in some edge cases using media-type?
                if "*" not in variables:
                    var_data = var_data[variables]
                if "*" not in datasource.data_variables:
                    var_cols = set(var_data.columns) - set(datasource.data_variables)
                    var_data = var_data[var_cols]
                if datasource.exclude_data_variables:
                    var_data = var_data.drop(columns=datasource.exclude_data_variables, errors='ignore')
                assets_data.append(var_data)
            item_df = pd.concat(assets_data, ignore_index=True)
            col_data.append(item_df)
            matched_zones.append(zoneIds[item.id])

        col = datasource._client.get_collection(datasource.collection_id)
        col_dims = self.get_dimensions_stac2dggs(col)

        col_data = pd.concat(col_data, ignore_index=True)
        col_meta = self.get_datadictionary(datasource_id, collection=col).data
        col_meta = {var: dtype for var, dtype in col_meta.items() if var in col_data.columns}

        result.data = col_data.to_numpy().tolist()
        result.zoneIds = matched_zones
        result.cols_meta = col_meta
        return result

    def get_dimensions_stac2dggs(self, collection: pystac.Collection) -> Dict[str, Dimension]:
        dims = {}
        for name, info in collection.to_dict().get("cube:dimensions", {}).items():
            grid = None
            # FIXME: no obvious mapping? what about temporal?
            #   (https://github.com/opengeospatial/ogcapi-discrete-global-grid-systems/issues/94)
            # step = info.get("step", None)
            # bbox = info.get("bbox", None)  # if type == "geometry"
            extent = info.get("extent", None)
            # values = info.get("values", None)
            # if step and info.get("type") == "spatial":  #  == "temporal":
            #     grid = {"coordinates": ...}
            dim = Dimension(
                name=name,
                interval=extent,
                grid=grid,
                unit=info.get("unit"),
                unitLang=info.get("reference_system"),
            )
            dims[name] = dim
        return dims

    def get_datadictionary(
        self,
        datasource_id: str,
        collection: Optional[pystac.Collection] = None,
    ) -> CollectionProviderGetDataDictReturn:
        result = CollectionProviderGetDataDictReturn(data={})
        datasource = self.get_source(datasource_id)
        if not datasource:
            return result

        try:
            col_obj = collection or datasource._client.get_collection(datasource.collection_id)
            col_data = col_obj.to_dict()
            cube_vars = col_data["cube:variables"]
            cube_data = {
                var: var_data["data_type"]
                for var, var_data in cube_vars.items()
                if (
                    var in datasource.data_variables
                    or "*" in datasource.data_variables
                    and var not in datasource.exclude_data_variables
                )
            }
        except Exception as e:
            logger.error(f'{__name__} {datasource_id} error: {e}')
            return result
        result.data = cube_data
        return result
