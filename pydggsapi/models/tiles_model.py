from pydantic import ValidationError
from pydggsapi.models.hytruck_model import querySuitability, queryModelledWeightsVariables
from pydggsapi.schemas.tiles.tiles import TilesFeatures, TilesJSON, VectorLayer
import pyproj
from shapely.geometry import box
from shapely.ops import transform
from uuid import UUID
from datetime import datetime
import os
import logging

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)

SRID_LNGLAT = 4326
SRID_SPHERICAL_MERCATOR = 3857
default_uuid = os.environ.get('DEFAULTUUID', '00000000-0000-0000-0000-000000000000')

transformer = pyproj.Transformer.from_crs(crs_from=SRID_LNGLAT, crs_to=SRID_SPHERICAL_MERCATOR, always_xy=True)


def get_suitability_features(client, param, dggrid, mercator):
    # input checking
    # the layer params come in "(kuuid)@(weights_setting_name)@(country_encoded)" format
    layer = param.layer
    layer = layer.split('@')
    if (len(layer) != 3):
        logging.error(f'{__name__} Layer isn\'t in correct format, using default setting')
        layer = [default_uuid, 'AHP Default Settings', -1]

    bbox, tile = mercator.getWGS84bbox(param.z, param.x, param.y)
    p = {
        "xmin": bbox.left,
        "ymin": bbox.bottom,
        "xmax": bbox.right,
        "ymax": bbox.top,
        # "epsg": tms.crs.to_epsg(),
    }
    logging.info(p)
    # check if tile is in cache
    # if not, query database and cache tile
    # if yes, return tile from cache

    # get zoom level from tile
    zoom_level = tile.z
    res_info = mercator.get(zoom_level)
    # tile width in lon deg to km ca, but is ca 0.5 at 60 deg
    tile_width_km = float(res_info["Tile width deg lons"]) / 0.01 * 0.4  # in km

    logging.info(f"z:{zoom_level} res_info: {tile_width_km}")

    # find the zoom level where the tile width is closest to the dggs cell width CLS
    dg_zoom = dggrid.find_zoom_by_cls_km(cls_km=tile_width_km)
    target_zoom = dg_zoom
    if dg_zoom < 5:
        target_zoom = 5
        dg_zoom = 5
    if dg_zoom > 9:
        target_zoom = 9
        dg_zoom = 9

    # ideally we arrive at a dg_zoom that is 2 levels plus but still within the dggs resolution range of max 9
    while target_zoom < 9:
        if target_zoom + 1 <= 9 and target_zoom + 1 <= dg_zoom + 1:
            target_zoom += 1
        else:
            break
    logging.info(f"dggs res:{dg_zoom} target res:{target_zoom}")

    clip_bound = box(bbox.left, bbox.bottom, bbox.right, bbox.top)
    project = pyproj.Transformer.from_crs(SRID_SPHERICAL_MERCATOR, SRID_LNGLAT, always_xy=True).transform
    clip_bound_wgs84 = transform(project, clip_bound)

    logging.info(clip_bound_wgs84.wkt)

    gdf1 = dggrid.generate_hexgrid(clip_bound_wgs84, target_zoom)
    select_cell_ids = gdf1.index.tolist()
    # the layer params come in "(kuuid)@(weights_setting_name)@(country_encoded)" format
    rs = querySuitability(client, select_cell_ids, target_zoom, layer[2], layer[0], layer[1])
    features_name = [i[0] for i in rs[1]]
    rs = rs[0]
    logging.info(f'{__name__} rs length : {len(rs)} , selected id len: {len(select_cell_ids)}')
    # exclude cellid when calc the weighted suitability
    features = []
    drop = []
    if (len(rs) > 0):
        for row in rs:
            cellid = int(row[0])
            if cellid in gdf1.index:
                p = {f: row[i] for i, f in enumerate(features_name)}
                features.append({"geometry": gdf1.loc[cellid, 'geometry'].wkt, 'properties': p})
            else:
                drop.append(row[0])
    logging.info(len(features))
    logging.info(f'{__name__} Not Found idx :{len(drop)}')
    return TilesFeatures(features=features), bbox


def get_tiles_json(client, baseurl, layer):
    response = queryModelledWeightsVariables(client)
    idx = [i for i, c in enumerate(response.return_.column) if (c[0] == 'layername')]
    variables_name = {v[idx[0]]: 'int' for v in response.return_.data}
    baseurl = "/".join(str(baseurl).split('/')[:-1]) + '/' + layer + '/{z}/{x}/{y}'
    bounds = [5.86307954788208, 47.31793212890625, 31.61196517944336, 70.0753173828125]
    description = "Suitability for hytruck project"
    return TilesJSON(**{'tilejson': '3.0.0', 'tiles': [baseurl], 'vector_layers': [VectorLayer(**{"id": layer, "fields": variables_name})],
                      'bounds': bounds, 'description': description, 'name': layer})


