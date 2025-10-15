from pydggsapi.dependencies.collections_providers.abstract_collection_provider import (
    AbstractCollectionProvider,
    AbstractDatasourceInfo
)
from pydggsapi.schemas.api.collection_providers import CollectionProviderGetDataReturn, CollectionProviderGetDataDictReturn

from pygeofilter.ast import AstType
from pygeofilter.backends.sql import to_sql_where

from pydantic import BaseModel
import xarray as xr
import xarray_sql as xql
from typing import List, Dict, Optional
from dataclasses import dataclass, field
import numpy as np
import logging

logger = logging.getLogger()


@dataclass
class ZarrDatasourceInfo(AbstractDatasourceInfo):
    filepath: str = ""
    filehandle: object = None
    # the column name of the zone ID, if not given,
    # it is assume to be the same with the zarr group name
    id_col: str = ""


# Zarr with Xarray DataTree
class ZarrCollectionProvider(AbstractCollectionProvider):

    def __init__(self, datasources):
        self.datasources = {}
        try:
            for k, v in datasources.items():
                datasource = ZarrDatasourceInfo(**v)
                datasource.filehandle = xr.open_datatree(datasource.filepath)
                self.datasources[k] = datasource
        except Exception as e:
            logger.error(f'{__name__} create datasource failed: {e}')
            raise Exception(f'{__name__} create datasource failed: {e}')

    def get_data(self, zoneIds: List[str], res: int, datasource_id: str, cql_filter: AstType = None) -> CollectionProviderGetDataReturn:
        datatree = None
        result = CollectionProviderGetDataReturn(zoneIds=[], cols_meta={}, data=[])
        try:
            datasource = self.datasources[datasource_id]
        except KeyError:
            logger.error(f'{__name__} {datasource_id} not found')
            raise ValueError(f'{__name__} {datasource_id} not found')
        try:
            zone_grp = datasource.zone_groups[str(res)]
        except KeyError as e:
            logger.error(f'{__name__} get zone_grp for resolution {res} failed: {e}')
            return result
        id_col = datasource.id_col if (datasource.id_col != "") else zone_grp
        datatree = datasource.filehandle[zone_grp]
        # in future, we may consider using xdggs-dggrid4py
        try:
            if (cql_filter is not None):
                fieldmapping = self.get_datadictionary(datasource_id).data
                fieldmapping = {k: k for k, v in fieldmapping.items()}
                sql = to_sql_where(cql_filter, fieldmapping)
                ctx = xql.XarrayContext()
                ds = datatree.to_dataset().chunk('auto')
                ctx.from_dataset('ds', ds)
                if ("*" in datasource.data_cols):
                    cols = f"* EXCLUDE({','.join(datasource.exclude_data_cols)})" if (len(datasource.exclude_data_cols) > 0) else "*"
                else:
                    cols_intersection = set(datasource.data_cols) - set(datasource.exclude_data_cols)
                    cols = f"{','.join(cols_intersection)}, {id_col}"
                sql = f"""select {cols} from ds where ("{id_col}" in ({', '.join(f"'{z}'" for z in zoneIds)})) and ({sql}) """
                zarr_result = xr.Dataset.from_dataframe(ctx.sql(sql).to_pandas().set_index(id_col))
            else:
                cols = set(datatree.data_vars) if ("*" in datasource.data_cols) else set(datasource.data_cols)
                cols = list(cols - set(datasource.exclude_data_cols))
                zarr_result = datatree.sel({id_col: np.array(zoneIds, dtype=datatree[zone_grp].dtype)})
                zarr_result = zarr_result.to_dataset()[cols]
        except Exception as e:
            # Zarr will raise exception if nothing matched
            logger.error(f'{__name__} datatree sel failed: {e}')
            return result
        cols_meta = {k: v.name for k, v in dict(zarr_result.data_vars.dtypes).items()}
        zarr_result = zarr_result.to_array()
        zoneIds = zarr_result[id_col].values.astype(str).tolist()
        data = zarr_result.data.T.tolist()
        result.zoneIds, result.cols_meta, result.data = zoneIds, cols_meta, data
        return result

    def get_datadictionary(self, datasource_id: str) -> CollectionProviderGetDataDictReturn:
        try:
            datatree = self.datasources[datasource_id]
        except KeyError as e:
            logger.error(f'{__name__} {datasource_id} not found: {e}.')
            raise Exception(f'{__name__} {datasource_id} not found: {e}.')
        datatree = datatree.filehandle[list(datatree.zone_groups.values())[0]]
        data = {i[0]: str(i[1].dtype) for i in datatree.data_vars.items()}
        return CollectionProviderGetDataDictReturn(data=data)


