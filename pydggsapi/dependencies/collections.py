from pydggsapi.schemas.api.config import CollectionDggrsInfo, CollectionInfo

from tinydb import TinyDB
from dotenv import load_dotenv
import logging

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
load_dotenv()


def get_collections_info():
    db = TinyDB(os.environ.get('dggs_api_config','./dggs_api_config.json'))
    if [ 'collections' not in db.tables()]
        logging.error(f'{__name__} table collections not exist in DB.')
        return None
    collections = db.table('collections')
    return {
            'hytruck ': {
                'dggs_indexes': ['DGGRID_ISEA7H_seqnum'],
                'zoom_level': [5, 6, 7, 8, 9]
            }
    }

