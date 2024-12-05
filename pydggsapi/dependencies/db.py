from clickhouse_driver import Client
from dggrid4py import DGGRIDv7

import os

# python tempdir / tempfile import
import tempfile
from uuid import UUID
from datetime import datetime
import logging
from copy import deepcopy

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s', datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


# Establishing the database client
def get_database_client_fine(use_numpy=False, compression='None'):
    return Client(host='127.0.0.1',
                  port=os.environ['CH_PORT'],
                  user=os.environ['CH_USER'],
                  password=os.environ['CH_PASS'],
                  database=os.environ['CH_DB'],
                  compression=compression,
                  settings={'use_numpy': use_numpy})


def get_database_client():
    return Client(host='127.0.0.1',
                  port=os.environ['CH_PORT'],
                  user=os.environ['CH_USER'],
                  password=os.environ['CH_PASS'],
                  database=os.environ['CH_DB'],
                  compression='lz4',
                  settings={'use_numpy': False})


def get_conformance_classes():
    return ["https://www.opengis.net/spec/ogcapi-common-1/1.0/conf/landing-page",
            "https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/core",
            "https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/zone-query",
            "https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/data-retrieval",
            "https://www.opengis.net/spec/ogcapi-dggs-1/1.0/conf/collection-dggs"]


def fetch_data_from_db(client, query, params, columnar=False):
    return client.execute(query, params, columnar=columnar)


def fetch_dataframe_from_db(client, query, params):
    return client.query_dataframe(query, params)

