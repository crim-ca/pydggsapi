from pydggsapi.dependencies.collections_providers.AbstractCollectionProvider import AbstractCollectionProvider
from pydggsapi.schemas.api.collection_providers import CollectionProviderGetDataReturn

from pydantic import BaseModel
import xarray as xr
from typing import List, Dict, Optional
import numpy as np
import logging

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


class Zarr_datasource_parameters(BaseModel):
    filepath: str
    zones_grps: Dict[str, str]
    filehandle: object = None


# Zarr with Xarray DataTree
class Zarr(AbstractCollectionProvider):
    datasources: Dict[str, Zarr_datasource_parameters] = {}

    def __init__(self, params):
        try:
            filelist = params.get('datasources')
            if (filelist is not None):
                for k, v in filelist.items():
                    param = Zarr_datasource_parameters(**v)
                    param.filehandle = xr.open_datatree(param.filepath)
                    self.datasources[k] = param
        except Exception as e:
            logging.error(f'{__name__} class initial failed: {e}')
            raise Exception(f'{__name__} class initial failed: {e}')

    def get_data(self, zoneIds: List[str], res: int, datasource_id: str, filepath: str = None,
                 zones_grps: Dict[int, str] = None) -> CollectionProviderGetDataReturn:
        datatree = None
        result = CollectionProviderGetDataReturn(zoneIds=[], cols_meta={}, data=[])
        try:
            datatree = self.datasources[datasource_id]
        except KeyError:
            try:
                param = Zarr_datasource_parameters(filepath, zones_grps)
                param.filehandle = xr.open_datatree(param.filepath)
                self.datasources[datasource_id] = param
                datatree = self.datasources[datasource_id]
                logging.info(f'{__name__} new datasource: {datasource_id} added.')
            except Exception as e:
                logging.error(f'{__name__} initial zarr collection failed: {e}')
                return result
        try:
            zone_grp = datatree.zones_grps[str(res)]
        except KeyError as e:
            logging.error(f'{__name__} get zone_grp for resolution {res} failed: {e}')
            return result
        datatree = datatree.filehandle[zone_grp]
        # in future, we may consider using xdggs-dggrid4py
        try:
            zarr_result = datatree.sel({f'{zone_grp}': np.array(zoneIds, dtype=datatree[zone_grp].dtype)})
        except Exception as e:
            logging.error(f'{__name__} datatree sel failed: {e}')
            return result
        cols_meta = {k: v.name for k, v in dict(zarr_result.data_vars.dtypes).items()}
        zarr_result = zarr_result.to_dataset().to_array()
        zoneIds = zarr_result[zone_grp].values.astype(str).tolist()
        data = zarr_result.data.T.tolist()
        result.zoneIds, result.cols_meta, result.data = zoneIds, cols_meta, data
        return result
