from pydggsapi.dependencies.collections_providers.AbstractCollectionProvider import AbstractCollectionProvider
from pydggsapi.schemas.api.collectionproviders import CollectionProviderGetDataReturn

from clickhouse_driver import Client
import numpy as np
import logging

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


class clickhouse(AbstractCollectionProvider):
    host: str
    port: int
    user: str
    password: str
    database: str

    def __init__(self, params):
        try:
            super().__init__(params['uid'], params['res_cols'], params['data_cols'])
            self.host = params['host']
            self.user = params['user']
            self.port = params['port']
            self.password = params['password']
            self.table = params['table']
            self.compression = params.get('compression', 'None')
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


    def get_data(self, zoneIds: List[str], res: int, aggregation: str='mode') -> CollectionProviderGetDataReturn:
        try:
            res_col = self.res_cols[res]
        except Exception as e:
            logging.error(f'{__name__} get res_cols for resolution {res} failed: {e}')
            raise Exception(f'{__name__} get res_cols for resolution {res} failed: {e}')
        if (aggregation == 'mode'):
            cols = [f'arrayMax(topK(1)({l})) as {l}' for l in self.data_cols]
            cols = ",".join(cols)
        cols += f', {res_col}'
        query = f'select {cols} from {table} where {res_col} in (%(cellid_list)s) group by {res_col}'
        param['cellid_list'] = cellIds
        result = client.execute(query, param, with_column_types=True)
        zone_idx = [i for i,r in enumerate(result[0]) if (r[0] == res_col)][0]
        data = np.array(result[1])
        zoneIds = data[:,zone_idx]
        data = np.delete(data, zone_idx, axis=-1)
        cols_name = [r[0] for r in result[0]]
        cols_dtype = [r[1] for r in result[0]]
        result = CollectionProviderGetDataReturn(zoneIds=zoneIds.tolist(), cols_name=cols_name, cols_dtype=cols_dtype, data=data.tolist())
        return result




