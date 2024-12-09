from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Link, LinkTemplate, LandingPageResponse
from pydggsapi.schemas.ogc_dggs.dggrs_list import DggrsItem, DggrsListResponse
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescription
from pydggsapi.schemas.ogc_dggs.dggrs_zones_info import ZoneInfoRequest, ZoneInfoResponse
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint


from typing import Dict
from pydggsapi.dependencies.dggs_isea7h import DggridISEA7H
from fastapi.exceptions import HTTPException
from uuid import UUID
from datetime import datetime
from copy import deepcopy
from pprint import pprint
import shapely
import os
import logging
logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


def _ISEA7H_zoomlevel_fromzoneId(zoneId, dggrid):
    # first, determinate the zoom level by
    # the first time it fail the relation if (zoom(num_of_cells) > cell id)
    # if all pass or all fail, set zoom level to 15
    zoom_level = list(dggrid.data.keys())
    zoom_level.sort()
    zoom_level = [(dggrid.data[z]['Cells'] > zoneId) for z in zoom_level]
    try:
        zoom_level = 0 if (all(zoom_level)) else zoom_level.index(True)
    except ValueError:
        # if all are False == max num of cells in resolution 15
        zoom_level = 15
    return zoom_level


def landingpage(current_url):
    root = '/'.join(str(current_url).split('/')[:-3])
    self_link = Link(**{'href': str(current_url), 'rel': 'self', 'type': 'application/json', 'title': 'Landing Page'})
    service_desc_link = Link(**{'href': root + '/docs', 'rel': 'service-desc', 'type': 'html', 'title': 'Open API swagger interface'})
    service_doc_link = Link(**{'href': 'https://docs.ogc.org/DRAFTS/21-038.html', 'rel': 'service-doc', 'type': 'html', 'title': 'API Documentation'})
    conformance_link = Link(**{'href': str(current_url) + 'conformance', 'rel': 'http://www.opengis.net/def/rel/ogc/1.0/conformance', 'type': 'application/json', 'title': 'Conformance classes implemented by this API.'})
    dggs_list_link =Link(**{'href': str(current_url) + 'dggs', 'rel': 'ogc-rel:dggrs-list', 'type': 'application/json', 'title': 'List of DGGS implemented by this API.'})
    return LandingPageResponse(**{'title': 'University of Tartu, OGC DGGS API v1-pre', 'description': 'ogc dggs api',
                                  'links': [self_link, service_desc_link, service_doc_link, conformance_link, dggs_list_link]})


def query_support_dggs(current_url, dggs_info: Dict[str, DggrsItem], filter_):
    # DGGRID_ISEA7H_seqnum
    logging.info(f'{__name__} support dggs')
    support_dggs = []
    for k, v in dggs_info.items():
        if (k in filter_):
            for i, link in enumerate(v.links):
                if link.rel == 'self':
                    v.links[i].href = str(current_url) + f'/{k}'
            support_dggs.append(v)
    logging.info(f'{__name__} support dggs ({len(support_dggs)})')
    landing_page = '/'.join(str(current_url).split('/')[:-1])
    dggs_landing_page = Link(**{'href': landing_page, 'rel': 'ogc-rel:dggrs-list', 'title': 'DGGS API landing page'})
    return DggrsListResponse(**{'links': [dggs_landing_page], 'dggrs': support_dggs})


def query_dggrs_definition(current_url, dggrs_description: DggrsDescription):
    logging.info(f'{__name__} query dggrs model {dggrs_description.id}')
    for i, link in enumerate(dggrs_description.links):
        if link.rel == 'self':
            dggrs_description.links[i].href = str(current_url) + f'/{dggrs_description.id}'
    zone_query_link = Link(**{'href': str(current_url) + '/zones', 'rel': 'ogc-rel:dggrs-zone-query', 'title': 'Dggrs zone-query link'})
    zone_data_link = LinkTemplate(**{'uriTemplate': str(current_url) + '/zones/{zoneId}/data', 'rel': 'ogc-rel:dggrs-zone-data',
                                     'title': 'Dggrs zone-query link'})
    dggrs_description.links.append(zone_query_link)
    dggrs_description.linkTemplates = [zone_data_link]
    logging.debug(f'{__name__} query dggrs model: {pprint(dggrs_description)}')
    return dggrs_description


def query_zone_info(zoneinfoReq: ZoneInfoRequest, current_url, dggs_info, dggrid: DggridISEA7H):
    logging.info(f'{__name__} query zone info {zoneinfoReq.dggrs_id}, zone id: {zoneinfoReq.zoneId}')
    if (zoneinfoReq.dggrs_id == 'DGGRID_ISEA7H_seqnum'):
        zoneId = zoneinfoReq.zoneId
        if (zoneId > dggrid.data[15]['Cells']):
            logging.error(f'{__name__} query zone info {zoneinfoReq.dggrs_id}, zone id {zoneinfoReq.zoneId} > max Zoomlevel number of zones')
            raise HTTPException(status_code=500, detial=f'{__name__} query zone info {zoneinfoReq.dggrs_id}, zone id {zoneinfoReq.zoneId} > max Zoomlevel number of zones')
        zoom_level = _ISEA7H_zoomlevel_fromzoneId(zoneId, dggrid)
        logging.info(f'{__name__} query zone info {zoneinfoReq.dggrs_id}, zone id: {zoneinfoReq.zoneId}, zoom level: {zoom_level}')
        try:
            centroid = dggrid.centroid_from_cellid([zoneId], zoom_level).geometry
            hex_geometry = dggrid.hexagon_from_cellid([zoneId], zoom_level).geometry
        except Exception:
            logging.error(f'{__name__} query zone info {zoneinfoReq.dggrs_id}, zone id {zoneinfoReq.zoneId} dggrid convert failed')
            raise HTTPException(status_code=500, detial=f'{__name__} query zone info {zoneinfoReq.dggrs_id}, zone id {zoneinfoReq.zoneId} dggrid convert failed')
        geometry, bbox = [], []
        for g in hex_geometry:
            geometry.append(GeoJSONPolygon(**eval(shapely.to_geojson(g))))
            bbox.append(list(g.bounds))
        dggs_link = '/'.join(str(current_url).split('/')[:-3])
        dggs_link = Link(**{'href': dggs_link, 'rel': 'ogc-rel:dggrs', 'title': 'Link back to /dggs (get list of supported dggs)'})
        data_link = Link(**{'href': str(current_url) + '/data', 'rel': 'ogc-rel:dggrs-zone-data', 'title': 'Link to data-retrieval for the zoneId)'})
        return_ = {'id': str(zoneId)}
        return_['level'] = zoom_level
        return_['links'] = [data_link, dggs_link]
        return_['shapeType'] = dggs_info['shapeType']
        return_['crs'] = dggs_info['crs']
        return_['centroid'] = GeoJSONPoint(**eval(shapely.to_geojson(centroid[0])))
        return_['bbox'] = bbox[0]
        return_['areaMetersSquare'] = dggrid.data[zoom_level]["Area (km^2)"] * 1000000
        return_['geometry'] = geometry[0]
        logging.debug(f'{__name__} query zone info {zoneinfoReq.dggrs_id}, zone id: {zoneinfoReq.zoneId}, zoneinfo: {pprint(return_)}')
        return ZoneInfoResponse(**return_)
    else:
        raise NotImplementedError(f'core (zone info) is not implemented for {zoneinfoReq.dggrs_id}')

