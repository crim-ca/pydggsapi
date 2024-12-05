from pydggsapi.schemas.ogc_dggs.dggrs_list import DggrsItem
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Link

from tinydb import TinyDB
from dotenv import load_dotenv
import logging
import os


logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)
load_dotenv()


def get_dggrs_indexes():
    db = TinyDB(os.environ.get('dggs_api_config', './dggs_api_config.json'))
    if ['dggrs' not in db.tables()]:
        logging.error(f'{__name__} table dggrs not exist in DB.')
        return None
    dggrs = db.table('dggrs')
    dggrs_dict = []
    for cid, v in dggrs.items():
        self_link = Link(**{'href': '', 'rel': 'self', 'title': 'Dggs Description link'})
        dggrs_model_link = Link(**{'href': '', 'rel': 'ogc-rel:dggrs-definition', 'title': 'DGGRS definition'})
        dggrs_dict[cid] = DggrsItem(id=cid, title=v['title'], links=[self_link, dggrs_model_link])
    return dggrs_dict
