from pydggsapi.dependencies.collections_providers.abstract_collection_provider import (
    AbstractCollectionProvider,
    AbstractDatasourceInfo
)
from pydggsapi.schemas.api.collection_providers import CollectionProviderGetDataReturn, CollectionProviderGetDataDictReturn
from dataclasses import dataclass
from clickhouse_driver import Client
from typing import List
import numpy as np
import logging

logger = logging.getLogger()


@dataclass
class ClickhouseDatasourceInfo(AbstractDatasourceInfo):
    table: str = "data"
    aggregation: str = "mode"


class ClickhouseCollectionProvider(AbstractCollectionProvider):

    def __init__(self, datasources):
        self.datasources = {}
        if (datasources.get("connection") is None):
            logger.error(f'{__name__} missing db connection info.')
            raise Exception(f'{__name__} missing db connection info.')
        connection = datasources["connection"]
        self.host: str = connection.get("host", "127.0.0.1")
        self.port: int = connection.get("port", 9000)
        self.user: str = connection.get("user", "user")
        self.password: str = connection.get("password", "user")
        self.database: str = connection.get("database", "default")
        self.compression: bool = connection.get("compression", False)
        try:
            self.db = Client(host=self.host, port=self.port, user=self.user, password=self.password,
                             database=self.database, compression=self.compression)
            datasources.pop("connection")
            for k, v in datasources.items():
                self.datasources[k] = ClickhouseDatasourceInfo(**v)
        except Exception as e:
            logger.error(f'{__name__} create datasource failed: {e}')
            raise Exception(f'{__name__} create datasource failed: {e}')

    def get_data(self, zoneIds: List[str], res: int, datasource_id: str) -> CollectionProviderGetDataReturn:
        result = CollectionProviderGetDataReturn(zoneIds=[], cols_meta={}, data=[])
        try:
            datasource = self.datasources[datasource_id]
        except KeyError:
            logger.error(f'{__name__} datasource_id not found: {datasource_id}')
            raise Exception(f'{__name__} datasource_id not found: {datasource_id}')
        try:
            res_col = datasource.zone_groups[str(res)]
        except KeyError as e:
            logger.error(f'{__name__} get zone_groups for resolution {res} failed: {e}')
            return result
        if (datasource.aggregation == 'mode'):
            cols = [f'arrayMax(topK(1)({c})) as {c}' for c in datasource.data_cols]
            cols = ",".join(cols)
        cols += f', {res_col}'
        query = f'select {cols} from {datasource.table} where {res_col} in (%(cellid_list)s) group by {res_col}'
        try:
            db_result = self.db.execute(query, {'cellid_list': zoneIds}, with_column_types=True)
        except Exception as e:
            logger.error(f'{__name__} get_data failed : {e}')
            raise Exception(f'{__name__} get_data failed : {e}')
        zone_idx = [i for i, r in enumerate(db_result[1]) if (r[0] == res_col)][0]
        if (len(db_result[0]) > 0):
            data = np.array(db_result[0])
            zoneIds = data[:, zone_idx].tolist()
            data = np.delete(data, zone_idx, axis=-1).tolist()
            cols_meta = {r[0]: r[1] for r in db_result[1] if (r[0] != res_col)}
            result.zoneIds, result.cols_meta, result.data = zoneIds, cols_meta, data
        return result

    def get_datadictionary(self, datasource_id: str) -> CollectionProviderGetDataDictReturn:
        try:
            datasource = self.datasources[datasource_id]
        except KeyError:
            logger.error(f'{__name__} datasource_id not found: {datasource_id}')
            raise Exception(f'{__name__} datasource_id not found: {datasource_id}')
        try:
            query = f'DESCRIBE TABLE {datasource.table}'
            db_result = self.db.execute(query)
        except Exception as e:
            logger.error(f'{__name__} get_datadictionary failed : {e}')
            raise Exception(f'{__name__} datasource_id not found: {datasource_id}')
        data = {r[0]: r[1] for r in db_result if (r[0] in datasource.data_cols)}
        data.update({'zone_id': 'string'})
        return CollectionProviderGetDataDictReturn(data=data)







