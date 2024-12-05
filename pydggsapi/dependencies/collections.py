from pydggsapi.schemas.api.config import CollectionDggrsInfo, Collection

from tinydb import TinyDB
from dotenv import load_dotenv
import logging
import os

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
load_dotenv()


def get_collections_info():
    db = TinyDB(os.environ.get('dggs_api_config', './dggs_api_config.json'))
    if ['collections' not in db.tables()]:
        logging.error(f'{__name__} table collections not exist in DB.')
        return None
    collections = db.table('collections')
    collections_dict = {}
    for cid, v in collections.items():
        dggrsidxs = []
        for k, v in collections['dggrs_indexes'].items():
            dggrsidxs.append(CollectionDggrsInfo(k, v))
        collections_dict[cid] = Collection(collectionid=k, dggrs_indexes=dggrsidxs, **v)
    return collections_dict
