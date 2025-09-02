from pydggsapi.dependencies.collections_providers.abstract_collection_provider import AbstractCollectionProvider
from pydggsapi.schemas.api.collection_providers import CollectionProviderGetDataReturn

from pystac_client import Client as STACClient
from pydantic import BaseModel
import xarray as xr
from typing import List, Dict, Optional
import numpy as np
import logging

logger = logging.getLogger()

class STAC_datasource_parameters(BaseModel):
    catalog_url: str
    collection_id: str
    zones_grps: Dict[str, str] = None

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

    def get_data(
        self,
        zoneIds: List[str],
        res: int,
        datasource_id: str,
        url: str = None,
        zones_grps: Dict[int | str, str] = None,
    ) -> CollectionProviderGetDataReturn:

        result = CollectionProviderGetDataReturn(zoneIds=[], cols_meta={}, data=[])
        try:
            datatree = self.datasources[datasource_id]
        except KeyError:
            try:
                param = STAC_datasource_parameters(url, zones_grps)
                param.filehandle = xr.open_datatree(param.filepath)
                self.datasources[datasource_id] = param
                datatree = self.datasources[datasource_id]
                logger.info(f'{__name__} new datasource: {datasource_id} added.')
            except Exception as e:
                logger.error(f'{__name__} initial zarr collection failed: {e}')
                return result
        try:
            zone_grp = datatree.zones_grps[str(res)]
        except KeyError as e:
            logger.error(f'{__name__} get zone_grp for resolution {res} failed: {e}')
            return result
        datatree = datatree.filehandle[zone_grp]
        # in future, we may consider using xdggs-dggrid4py
        try:
            zarr_result = datatree.sel({f'{zone_grp}': np.array(zoneIds, dtype=datatree[zone_grp].dtype)})
        except Exception as e:
            logger.error(f'{__name__} datatree sel failed: {e}')
            return result
        cols_meta = {k: v.name for k, v in dict(zarr_result.data_vars.dtypes).items()}
        zarr_result = zarr_result.to_dataset().to_array()
        zoneIds = zarr_result[zone_grp].values.astype(str).tolist()
        data = zarr_result.data.T.tolist()
        result.zoneIds, result.cols_meta, result.data = zoneIds, cols_meta, data
        return result
