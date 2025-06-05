from pydggsapi.schemas.api.collection_providers import CollectionProvider

from tinydb import TinyDB
import logging
import os

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


def get_collection_providers():
    db = TinyDB(os.environ.get('dggs_api_config'))
    if ('collection_providers' not in db.tables()):
        logging.error(f'{__name__} collection_providers table not found.')
        raise Exception(f'{__name__} collection_providers table not found.')
    providers = db.table('collection_providers').all()
    if (len(providers) == 0):
        logging.error(f'{__name__} no collection_providers defined.')
        raise Exception(f'{__name__} no collection_providers defined.')
    providers_dict = {}
    for provider in providers:
        id_, provider_config = provider.popitem()
        providers_dict[id_] = CollectionProvider(**provider_config)
    return providers_dict

