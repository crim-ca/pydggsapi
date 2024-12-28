from pydggsapi.schemas.api.config import Collection

from tinydb import TinyDB
from dotenv import load_dotenv
import logging
import os

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
load_dotenv()


def get_collections_info():
    db = TinyDB(os.environ.get('dggs_api_config', './dggs_api_config.json'))
    if ('collections' not in db.tables()):
        logging.error(f'{__name__} table collections not exist in DB.')
        return None
    collections = db.table('collections')
    collections_dict = {}
    for collection in collections:
        cid, collection_config = collection.popitem()
        collection_config['collectionid'] = cid
        collections_dict[cid] = Collection(**collection_config)
    return collections_dict

