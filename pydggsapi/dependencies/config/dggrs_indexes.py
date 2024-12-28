from pydggsapi.schemas.ogc_dggs.dggrs_list import DggrsItem
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescription
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Link
from pydggsapi.dependencies.config.collections import get_collections_info

from typing import Dict
from tinydb import TinyDB
from dotenv import load_dotenv
import logging
import os


logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
load_dotenv()


def _checkIfTableExists():
    db = TinyDB(os.environ.get('dggs_api_config', './dggs_api_config.json'))
    if ('dggrs' not in db.tables()):
        logging.error(f'{__name__} No dggrs definition is found.')
        raise Exception(f"{__name__} No dggrs definition is found.")
    return db


def get_dggrs_items() -> Dict[str, DggrsItem]:
    try:
        db = _checkIfTableExists()
    except Exception:
        return None
    dggrs_indexes = db.table('dggrs')
    dggrs_dict = {}
    for dggrs in dggrs_indexes:
        dggrsid, dggrs_config = dggrs.popitem()
        self_link = Link(**{'href': '', 'rel': 'self', 'title': 'DGGRS description link'})
        dggrs_model_link = Link(**{'href': dggrs_config['definition_link'], 'rel': 'ogc-rel:dggrs-definition', 'title': 'DGGRS definition'})
        dggrs_dict[dggrsid] = DggrsItem(id=dggrsid, title=dggrs_config['title'], links=[self_link, dggrs_model_link])
    return dggrs_dict


def get_dggrs_class(dggrsId: str) -> str:
    try:
        db = _checkIfTableExists()
    except Exception:
        return None
    dggrs_indexes = db.table('dggrs')
    for dggrs in dggrs_indexes:
        id_, dggrs_config = dggrs.popitem()
        if (id_ == dggrsId):
            return dggrs_config['classname']
    return None


def get_dggrs_descriptions() -> Dict[str, DggrsDescription]:
    try:
        db = _checkIfTableExists()
    except Exception:
        return None
    dggrs_indexes = db.table('dggrs')
    dggrs_dict = {}
    collections = get_collections_info()
    collections = {} if (collections is None) else collections
    tmp = [v.dggrs_indexes for k, v in collections.items()]
    max_dggrs = {}
    for t in tmp:
        for dggrs_key, zone_level in t.items():
            if (dggrs_key in max_dggrs.keys()):
                local_max = max(zone_level)
                max_dggrs[dggrs_key] = local_max if (max_dggrs[dggrs_key] < local_max) else max_dggrs[dggrs_key]
    for dggrs in dggrs_indexes:
        dggrsid, dggrs_config = dggrs.popitem()
        self_link = Link(**{'href': '', 'rel': 'self', 'title': 'DGGRS description link'})
        dggrs_model_link = Link(**{'href': dggrs_config['definition_link'], 'rel': 'ogc-rel:dggrs-definition', 'title': 'DGGRS definition'})
        dggrs_config['id'] = dggrsid
        dggrs_config['maxRefinementLevel'] = max_dggrs.get(dggrsid, 32)
        dggrs_config['links'] = [self_link, dggrs_model_link]
        del dggrs_config['definition_link']
        dggrs_dict[dggrsid] = DggrsDescription(**dggrs_config)
    return dggrs_dict
