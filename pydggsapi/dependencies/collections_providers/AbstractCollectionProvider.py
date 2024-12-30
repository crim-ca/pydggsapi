from pydggsapi.schemas.api.collectionproviders import CollectionProviderGetDataReturn

from abc import ABC, abstractmethod
from typing import List, Any, Union, Dict


class AbstractCollectionProvider(ABC):
    res_cols = {}
    data_cols = {}
    uid: str

    def __init__(self, uid, res_cols: Dict[int, str], data_cols: List[str]):
        self.res_cols = res_cols
        self.data_cols = data_cols
        self.uid = uid

    # 1. The return data must be aggregated.
    # 2. The return consist of 4 parts (zoneIds, cols_name, cols_dtype, data)
    # 3. The zoneIds is the list of zoneID , its length must align with data's length
    # 4. cols_name and cols_dtype lenght must align
    # 5. data is the data :P
    @abstractmethod
    def get_data(self, zoneIds: List[str], res: int) -> CollectionProviderGetDataReturn:
        raise NotImplementedError


