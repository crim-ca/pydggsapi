from pydggsapi.schemas.api.collections import Collection

from tinydb import TinyDB
import logging
import os

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


def get_collections_info():
    db = TinyDB(os.environ.get('dggs_api_config'))
    if ('collections' not in db.tables()):
        logging.error(f'{__name__} collections table not found.')
        raise Exception(f'{__name__} collections table not found.')
    collections = db.table('collections').all()
    if (len(collections) == 0):
        logging.warning(f'{__name__} no collections defined.')
        # raise Exception(f'{__name__} no collections defined.')
    collections_dict = {}
    for collection in collections:
        cid, collection_config = collection.popitem()
        collection_config['collectionid'] = cid
        collections_dict[cid] = Collection(**collection_config)
    return collections_dict

