from pydggsapi.dependencies.collections_providers.abstract_collection_provider import AbstractCollectionProvider
from pydggsapi.schemas.api.collection_providers import CollectionProviderGetDataReturn

from clickhouse_driver import Client
from typing import List
import numpy as np
import logging

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


class ClickhouseCollectionProvider(AbstractCollectionProvider):
    host: str
    port: int
    user: str
    password: str
    database: str

    def __init__(self, params):
        try:
            self.host = params['host']
            self.user = params['user']
            self.port = params['port']
            self.password = params['password']
            self.compression = params.get('compression', False)
            self.database = params.get('database', 'default')
        except Exception as e:
            logging.error(f'{__name__} class initial failed: {e}')
            raise Exception(f'{__name__} class initial failed: {e}')
        try:
            self.db = Client(host=self.host, port=self.port, user=self.user, password=self.password,
                             database=self.database, compression=self.compression)
        except Exception as e:
            logging.error(f'{__name__} class initial failed: {e}')
            raise Exception(f'{__name__} class initial failed: {e}')

    def get_data(self, zoneIds: List[str], res: int, table, zoneId_cols, data_cols, aggregation: str = 'mode') -> CollectionProviderGetDataReturn:
        result = CollectionProviderGetDataReturn(zoneIds=[], cols_meta={}, data=[])
        try:
            res_col = zoneId_cols[str(res)]
        except KeyError as e:
            logging.error(f'{__name__} get zoneId_cols for resolution {res} failed: {e}')
            return result
        if (aggregation == 'mode'):
            cols = [f'arrayMax(topK(1)({l})) as {l}' for l in data_cols]
            cols = ",".join(cols)
        cols += f', {res_col}'
        query = f'select {cols} from {table} where {res_col} in (%(cellid_list)s) group by {res_col}'
        db_result = self.db.execute(query, {'cellid_list': zoneIds}, with_column_types=True)
        zone_idx = [i for i, r in enumerate(db_result[1]) if (r[0] == res_col)][0]
        if (len(db_result[0]) > 0):
            data = np.array(db_result[0])
            zoneIds = data[:, zone_idx].tolist()
            data = np.delete(data, zone_idx, axis=-1).tolist()
            cols_meta = {r[0]: r[1] for r in db_result[1] if (r[0] != res_col)}
            result.zoneIds, result.cols_meta, result.data = zoneIds, cols_meta, data
        return result




