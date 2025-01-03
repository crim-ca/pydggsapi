from pydantic import ValidationError
from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Link, LinkTemplate
from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import ZonesDataRequest, Property, Value, ZonesDataDggsJsonResponse, Feature, ZonesDataGeoJson
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint
from pydggsapi.schemas.api.dggsproviders import DGGRSProviderZonesElement

from pydggsapi.dependencies.dggrs_providers.AbstractDGGRS import AbstractDGGRS
from pydggsapi.dependencies.collections_providers.AbstractCollectionProvider import AbstractCollectionProvider

from fastapi.responses import FileResponse
from numcodecs import Blosc
from typing import List
import shapely
import tempfile
import numpy as np
import zarr
import geopandas as gpd
import pandas as pd
import os
import json
import logging
logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


def query_zone_data(zoneId: str | int, zone_levels: List[int], dggrsId: str, dggrslink: str, dggrid: AbstractDGGRS,
                    collectionproviders: List[AbstractCollectionProvider],
                    returntype='application/dggs-json', returngeometry='zone-region', exclude=True):
    logging.info(f'{__name__} query zone data {dggrsId}, zone id: {zoneId}, zonelevel: {zone_levels}, return: {returntype}, geometry: {returngeometry}')
    # generate cell ids, geometry for relative_depth
    result = dggrid.get_relative_zonelevels(zoneId, zone_levels[0], zone_levels[1:], returngeometry)
    if (exclude is False):
        parent = dggrid.zonesinfo([zoneId])
        g = parent.geometry[0] if (returngeometry == 'zone-region') else parent.centroids[0]
        result.relative_zonelevels[zone_levels[0]] = DGGRSProviderZonesElement(**{'zoneIds': [zoneId], 'geometry': [g]})
    # get data and form a master dataframe (seleceted providers) for each zone level
    data = {}
    data_type = {}
    for z, v in result.relative_zonelevels.items():
        g = [shapely.from_geojson(json.dumps(g.__dict__))for g in v.geometry]
        master = gpd.GeoDataFrame(v.zoneIds, geometry=g, columns=['zoneId']).set_index('zoneId')
        idx = master.index.values.tolist()
        for cp in collectionproviders:
            collection_result = cp.get_data(idx, z)
            cols_name = {f'{cp.uid}_{k}': v for k, v in collection_result.cols_meta.items()}
            data_type.update(cols_name)
            id_ = np.array(collection_result.zoneIds).reshape(-1, 1)
            tmp = pd.DataFrame(np.concatenate([id_, collection_result.data], axis=-1),
                               columns=['zoneId'] + list(cols_name.keys())).set_index('zoneId')
            master = master.join(tmp)
        data[z] = master if (returntype == 'application/geo+json') else master.drop(columns=['geometry']).T
    zarr_root, tmpfile = None, None
    features = []
    id_ = 0
    properties, values = {}, {}
    if (returntype == 'application/zarr+zip'):
        tmpfile = tempfile.mkstemp()
        zipstore = zarr.ZipStore(tmpfile[1], mode='w')
        zarr_root = zarr.group(zipstore)

    for z, d in data.items():
        if (returntype == 'application/geo+json'):
            d.reset_index(inplace=True)
            geometry = d['geometry'].values
            geojson = GeoJSONPolygon if (returngeometry == 'zone-region') else GeoJSONPoint
            d = d.drop(columns='geometry')
            feature = d.to_dict(orient='records')
            feature = [Feature(**{'type': "Feature", 'id': id_ + i, 'geometry': geojson(**shapely.geometry.mapping(geometry[i])), 'properties': f}) for i, f in enumerate(feature)]
            features += feature
            id_ += len(d)
            logging.info(f'{__name__} query zone data {dggrsId}, zone id: {zoneId}@{z}, geo+json features len: {len(features)}')
        else:
            v = d.values
            if (len(properties.keys()) == 0):
                properties = {c: Property(**{'type': data_type[c]}) for c in d.index}
            if (len(values.keys()) == 0):
                for c in properties.keys():
                    values[c] = []
            for i, column in enumerate(d.index):
                values[column].append(Value(**{'depth': z, 'shape': {'count': len(v[i, :])}, "data": v[i, :].tolist()}))
                if (zarr_root is not None):
                    root = zarr_root
                    if (z != zone_levels[0]):
                        if (f'zone_level_{z}' not in zarr_root.group_keys()):
                            root = zarr_root.create_group(f'zone_level_{z}')
                            root.attrs.update({k: v.__dict__ for k, v in properties.items()})
                        else:
                            root = zarr_root[f'zone_level_{z}']
                    compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
                    sub_zarr = root.create_dataset(f'{column}_zone_level_' + str(z), data=v[i, :], compressor=compressor)
                    sub_zarr.attrs.update({'_ARRAY_DIMENSIONS': ['zoneId']})
    if (zarr_root is not None):
        zarr_root.attrs.update({k: v.__dict__ for k, v in properties.items()})
        zarr.consolidate_metadata(zipstore)
        zipstore.close()
        return FileResponse(tmpfile[1], headers={'content-type': 'application/zarr+zip'})
    if (returntype == 'application/geo+json'):
        return ZonesDataGeoJson(**{'type': 'FeatureCollection', 'features': features})

    return_ = {'dggrs': dggrslink, 'zoneId': str(zoneId), 'depths': zone_levels if (exclude is False) else zone_levels[1:],
               'properties': properties, 'values': values}
    return ZonesDataDggsJsonResponse(**return_)













