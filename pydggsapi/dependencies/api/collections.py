from pydggsapi.schemas.api.collections import Collection
from pydggsapi.schemas.ogc_collections.collections import Extent
from pydggsapi.schemas.ogc_collections.extent import Spatial, Temporal

from tinydb import TinyDB
import logging
import os

logger = logging.getLogger()


def get_collections_info() -> dict[str, Collection]:
    db = TinyDB(os.environ.get('dggs_api_config'))
    if ('collections' not in db.tables()):
        logger.error(f'{__name__} collections table not found.')
        raise Exception(f'{__name__} collections table not found.')
    collections = db.table('collections').all()
    if (len(collections) == 0):
        logger.warning(f'{__name__} no collections defined.')
        # raise Exception(f'{__name__} no collections defined.')
    collections_dict = {}
    for collection in collections:
        cid, collection_config = collection.popitem()
        if (collection_config.get('extent') is not None):
            spatial = Spatial(**collection_config['extent'].get('spatial', {}))
            temporal = Temporal(**collection_config['extent']['temporal']) if collection_config['extent'].get('temporal') else None
            collection_config['extent'] = Extent(spatial=spatial, temporal=temporal)
        collection_config['id'] = cid
        collections_dict[cid] = Collection(**collection_config)
    return collections_dict
