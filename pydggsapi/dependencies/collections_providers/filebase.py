from pydggsapi.dependencies.collections_providers.AbstractCollectionProvider import AbstractCollectionProvider
from pydggsapi.schemas.api.collection_providers import CollectionProviderGetDataReturn

from pydantic import BaseModel
import xarray as xr
from typing import List, Dict, Optional
import numpy as np
import logging

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


class Zarr_collection_parameters(BaseModel):
    filepath: str
    zones_grps: Dict[int, str]
    variable_cols: Optional[List[str]] = None
    filehandle: object = None


# Zarr with Xarray DataTree
class Zarr(AbstractCollectionProvider):
    collections: Dict[str, Zarr_collection_parameters] = {}

    def __init__(self, params):
        try:
            filelist = params.get('filelist')
            if (filelist is not None):
                for k, v in filelist.items():
                    param = Zarr_collection_parameters(**v)
                    param.filehandle = xr.open_datatree(param.filepath)
                    self.collections[k] = param
        except Exception as e:
            logging.error(f'{__name__} class initial failed: {e}')
            raise Exception(f'{__name__} class initial failed: {e}')

    def get_data(self, zoneIds: List[str], res: int, collection_name: str, filepath: str = None,
                 zones_grps: Dict[int, str] = None, variable_cols: list = None) -> CollectionProviderGetDataReturn:
        try:
            self.collections[collection_name]
        except KeyError:
            try:
                param = Zarr_collection_parameters(filepath, zones_grps, variable_cols)
                param.filehandle = xr.open_datatree(param.filepath)
                self.collections[collection_name] = param
            except Exception as e:
                logging.error(f'{__name__} initial zarr collection failed: {e}')
                raise Exception(f'{__name__} initial zarr collection failed: {e}')
        zarr_obj = self.collections[collection_name]
        zone_grp = zarr_obj.zones_grps[res]
        datatree = zarr_obj.filehandle[zone_grp]
        # in future, we may consider using xdggs-dggrid4py
        result = datatree.sel({f'{zones_grps}': np.array(zoneIds, dtype=zone_grp.dtype)})
        result = result.to_dataset().to_array()
        if (len(result[0]) > 0):
            data = np.array(result[0])
            zoneIds = data[:, zone_idx].tolist()
            data = np.delete(data, zone_idx, axis=-1).tolist()
            cols_meta = {r[0]: r[1] for r in result[1] if (r[0] != res_col)}
        result = CollectionProviderGetDataReturn(zoneIds=zoneIds, cols_meta=cols_meta, data=data)
        return result




