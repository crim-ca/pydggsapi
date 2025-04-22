from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescription
from pydggsapi.schemas.ogc_dggs.dggrs_zones_data import Property, Value, ZonesDataDggsJsonResponse, Feature, ZonesDataGeoJson
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint
from pydggsapi.schemas.api.dggrs_providers import DGGRSProviderZonesElement
from pydggsapi.schemas.api.collections import Collection

from pydggsapi.dependencies.dggrs_providers.AbstractDGGRS import AbstractDGGRS
from pydggsapi.dependencies.collections_providers.AbstractCollectionProvider import AbstractCollectionProvider

from fastapi.responses import FileResponse
from urllib import parse
from numcodecs import Blosc
from typing import List, Dict
from scipy.stats import mode
import shapely
import tempfile
import numpy as np
import zarr
import geopandas as gpd
import pandas as pd
import json
import logging
logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


def query_zone_data(zoneId: str | int, zone_levels: List[int], dggrs_description: DggrsDescription, dggrs_provider: AbstractDGGRS,
                    collection: Dict[str, Collection], collection_provider: List[AbstractCollectionProvider],
                    returntype='application/dggs-json', returngeometry='zone-region', exclude=True):
    logging.info(f'{__name__} query zone data {dggrs_description.id}, zone id: {zoneId}, zonelevel: {zone_levels}, return: {returntype}, geometry: {returngeometry}')
    # generate cell ids, geometry for relative_depth
    result = dggrs_provider.get_relative_zonelevels(zoneId, zone_levels[0], zone_levels[1:], returngeometry)
    if (exclude is False):
        parent = dggrs_provider.zonesinfo([zoneId])
        g = parent.geometry[0] if (returngeometry == 'zone-region') else parent.centroids[0]
        result.relative_zonelevels[zone_levels[0]] = DGGRSProviderZonesElement(**{'zoneIds': [zoneId], 'geometry': [g]})
    # get data and form a master dataframe (seleceted providers) for each zone level
    data = {}
    data_type = {}
    for cid, c in collection.items():
        convert = False
        cp = collection_provider[c.collection_provider.providerId]
        getdata_params = c.collection_provider.getdata_params
        if (c.collection_provider.dggrsId != dggrs_description.id):
            convert = True
        for z, v in result.relative_zonelevels.items():
            g = [shapely.from_geojson(json.dumps(g.__dict__))for g in v.geometry]
            org_z = z
            if (convert):
                converted = dggrs_provider.convert(v.zoneIds, c.collection_provider.dggrsId)
                tmp = gpd.GeoDataFrame({'vid': v.zoneIds}, geometry=g).set_index('vid')
                master = pd.DataFrame({'vid': converted.virtual_zoneIds, 'zoneId': converted.actual_zoneIds}).set_index('vid')
                master = master.join(tmp).reset_index().set_index('zoneId')
                z = converted.actual_res[0]
            else:
                master = gpd.GeoDataFrame(v.zoneIds, geometry=g, columns=['zoneId']).set_index('zoneId')
            idx = master.index.values.tolist()
            collection_result = cp.get_data(idx, z, **getdata_params)
            if (len(collection_result.zoneIds) > 0):
                cols_name = {f'{cid}.{k}': v for k, v in collection_result.cols_meta.items()}
                data_type.update(cols_name)
                id_ = np.array(collection_result.zoneIds).reshape(-1, 1)
                tmp = pd.DataFrame(np.concatenate([id_, collection_result.data], axis=-1),
                                   columns=['zoneId'] + list(cols_name.keys())).set_index('zoneId')
                master = master.join(tmp)
                master = master.astype(cols_name)
                if (convert):
                    master.reset_index(inplace=True)
                    tmp_geo = master.groupby('vid')['geometry'].last()
                    master.drop(columns=['zoneId', 'geometry'], inplace=True)
                    master = master.groupby('vid').agg(lambda x: mode(x)[0])
                    master = master.join(tmp_geo).reset_index().rename(columns={'vid': 'zoneId'})
                    master.set_index('zoneId', inplace=True)
                master = master if (returntype == 'application/geo+json') else master.drop(columns=['geometry'])
                try:
                    data[org_z] = data[org_z].join(master, rsuffix=cid)
                    data[org_z] = data[org_z].drop(columns=[f'geometry{cid}'], errors='ignore')
                except KeyError:
                    data[org_z] = master
    if (len(data.keys()) == 0):
        return None
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
            d['depth'] = z
            feature = d.to_dict(orient='records')
            feature = [Feature(**{'type': "Feature", 'id': id_ + i, 'geometry': geojson(**shapely.geometry.mapping(geometry[i])), 'properties': f}) for i, f in enumerate(feature)]
            features += feature
            id_ += len(d)
            logging.info(f'{__name__} query zone data {dggrs_description.id}, zone id: {zoneId}@{z}, geo+json features len: {len(features)}')
        else:
            zoneIds = d.index.values.astype(str).tolist()
            d = d.T
            v = d.values
            diff = set(list(d.index)) - set(list(properties.keys()))
            properties.update({c: Property(**{'type': data_type[c]}) for c in diff})
            diff = set(list(d.index)) - set(list(values.keys()))
            values.update({c: [] for c in diff})
            for i, column in enumerate(d.index):
                values[column].append(Value(**{'depth': z, 'shape': {'count': len(v[i, :])}, "data": v[i, :].tolist()}))
                if (zarr_root is not None):
                    root = zarr_root
                    if (f'zone_level_{z}' not in zarr_root.group_keys()):
                        root = zarr_root.create_group(f'zone_level_{z}')
                    else:
                        root = zarr_root[f'zone_level_{z}']
                    compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
                    if ('zoneId' not in root.array_keys()):
                        sub_zarr = root.create_dataset('zoneId', data=zoneIds, compressor=compressor)
                        sub_zarr.attrs.update({'_ARRAY_DIMENSIONS': ["zoneId"]})
                    sub_zarr = root.create_dataset(f'{column}_zone_level_' + str(z), data=v[i, :].astype(data_type[column].lower()), compressor=compressor)
                    sub_zarr.attrs.update({'_ARRAY_DIMENSIONS': ["zoneId"]})
    if (zarr_root is not None):
        zarr_root.attrs.update({k: v.__dict__ for k, v in properties.items()})
        zarr.consolidate_metadata(zipstore)
        zipstore.close()
        return FileResponse(tmpfile[1], headers={'content-type': 'application/zarr+zip'})
    if (returntype == 'application/geo+json'):
        return ZonesDataGeoJson(**{'type': 'FeatureCollection', 'features': features})
    link = [k.href for k in dggrs_description.links if (k.rel == 'ogc-rel:dggrs-definition')][0]
    return_ = {'dggrs': link, 'zoneId': str(zoneId), 'depths': zone_levels if (exclude is False) else zone_levels[1:],
               'properties': properties, 'values': values}
    return ZonesDataDggsJsonResponse(**return_)













