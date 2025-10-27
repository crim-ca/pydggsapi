from pydggsapi.dependencies.collections_providers.abstract_collection_provider import (
    AbstractCollectionProvider,
    AbstractDatasourceInfo
)
from pydggsapi.schemas.api.collection_providers import CollectionProviderGetDataReturn, CollectionProviderGetDataDictReturn

from dataclasses import dataclass
import xarray as xr
from typing import List
import numpy as np
import logging

logger = logging.getLogger()


@dataclass
class ZarrDatasourceInfo(AbstractDatasourceInfo):
    filepath: str = ""
    filehandle: object = None
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

    def get_data(self, zoneIds: List[str], res: int, datasource_id: str) -> CollectionProviderGetDataReturn:
        datatree = None
        result = CollectionProviderGetDataReturn(zoneIds=[], cols_meta={}, data=[])
        try:
            datatree = self.datasources[datasource_id]
        except KeyError:
            logger.error(f'{__name__} datasource not found: {datasource_id}')
            raise Exception(f'{__name__} datasource not found: {datasource_id}')
        try:
            zone_group = datatree.zone_groups[str(res)]
        except KeyError:
            logger.error(f'{__name__} get zone_groups for resolution {res} failed.')
            return result
            # raise ValueError(f'{__name__} get zone_groups for resolution {res} failed.')
        id_col = datatree.id_col if (datatree.id_col != "") else zone_group
        datatree = datatree.filehandle[zone_group]
        # in future, we may consider using xdggs-dggrid4py
        try:
            zarr_result = datatree.sel({id_col: np.array(zoneIds, dtype=datatree[id_col].dtype)})
        except Exception as e:
            # Zarr will raise exception if nothing matched
            logger.error(f'{__name__} datatree sel failed: {e}')
            return result
        cols_meta = {k: v.name for k, v in dict(zarr_result.data_vars.dtypes).items()}
        zarr_result = zarr_result.to_dataset().to_array()
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
        data.update({'zone_id': 'string'})
        return CollectionProviderGetDataDictReturn(data=data)




