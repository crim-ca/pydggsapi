from pydggsapi.schemas.ogc_dggs.common_ogc_dggs_api import Link, LinkTemplate, LandingPageResponse
from pydggsapi.schemas.ogc_dggs.dggrs_list import DggrsItem, DggrsListResponse
from pydggsapi.schemas.ogc_dggs.dggrs_descrption import DggrsDescription
from pydggsapi.schemas.ogc_dggs.dggrs_zones_info import ZoneInfoRequest, ZoneInfoResponse
from pydggsapi.schemas.api.collections import Collection
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint
from pydggsapi.dependencies.collections_providers.abstract_collection_provider import AbstractCollectionProvider
from pydggsapi.dependencies.dggrs_providers.abstract_dggrs_provider import AbstractDGGRSProvider

from fastapi import FastAPI
from starlette.requests import URL
from urllib.parse import urljoin

from typing import Dict
from pprint import pprint
import logging
import os

logger = logging.getLogger()


def landingpage(current_url: URL, app: FastAPI) -> LandingPageResponse:
    base_url = str(current_url)
    root_url = urljoin(base_url, "/")
    self_link = Link(href=base_url, rel='self', type='application/json', title='Landing Page')
    service_desc_link = Link(href=urljoin(root_url, app.openapi_url), rel='service-desc', type='application/json', title='OpenAPI specification')
    service_doc_link = Link(href=urljoin(root_url, app.docs_url), rel='service-desc', type='text/html', title='OpenAPI swagger interface')
    described_by_link = Link(href='https://docs.ogc.org/DRAFTS/21-038.html', rel='describedby', type='text/html', title='API Documentation')
    conformance_link = Link(href=urljoin(base_url, './conformance'), rel='http://www.opengis.net/def/rel/ogc/1.0/conformance',
                            type='application/json', title='Conformance classes implemented by this API.')
    dggs_list_link =Link(href=urljoin(base_url, './dggs'), rel='[ogc-rel:dggrs-list]', type='application/json',
                         title='List of DGGS implemented by this API.')
    links = [self_link, service_desc_link, service_doc_link, described_by_link, conformance_link, dggs_list_link]
    service_meta_url = os.environ.get('SERVICE_META_URL', None)
    if service_meta_url:
        service_meta_link = Link(href=service_meta_url, rel='service-meta', type='application/json', title='API metadata')
        links.append(service_meta_link)
    return LandingPageResponse(title=app.title, description=app.description, links=links)


def query_support_dggs(current_url, selected_dggrs: Dict[str, DggrsDescription]):
    # DGGRID_ISEA7H_seqnum
    logger.debug(f'{__name__} support dggs')
    support_dggrs = []
    for k, v in selected_dggrs.items():
        for i, link in enumerate(v.links):
            if link.rel == 'self':
                v.links[i].href = str(current_url) + f'/{k}'
        support_dggrs.append(DggrsItem(id=k, title=v.title, links=v.links))
    logger.debug(f'{__name__} support dggs ({len(support_dggrs)})')
    landing_page = '/'.join(str(current_url).split('/')[:-1])
    dggs_landing_page = Link(href=landing_page, rel='[ogc-rel:dggrs-list]', title='DGGS API landing page')
    return DggrsListResponse(**{'links': [dggs_landing_page], 'dggrs': support_dggrs})


def query_dggrs_definition(current_url, dggrs_description: DggrsDescription):
    logger.debug(f'{__name__} query dggrs model {dggrs_description.id}')
    for i, link in enumerate(dggrs_description.links):
        if link.rel == 'self':
            dggrs_description.links[i].href = str(current_url)
    zone_query_link = Link(href=str(current_url) + '/zones', rel='[ogc-rel:dggrs-zone-query]', title='Dggrs zone-query link')
    zone_data_link = LinkTemplate(uriTemplate=str(current_url) + '/zones/{zoneId}/data', rel='[ogc-rel:dggrs-zone-data]',
                                     title='Dggrs zone-query link')
    dggrs_description.links.append(zone_query_link)
    dggrs_description.linkTemplates = [zone_data_link]
    logger.debug(f'{__name__} query dggrs model: {pprint(dggrs_description)}')
    return dggrs_description


def query_zone_info(zoneinfoReq: ZoneInfoRequest, current_url, dggs_info: DggrsDescription, dggrs_provider: AbstractDGGRSProvider,
                    collection: Dict[str, Collection], collection_provider: Dict[str, AbstractCollectionProvider]):
    logger.debug(f'{__name__} query zone info {zoneinfoReq.dggrsId}, zone id: {zoneinfoReq.zoneId}')
    zoneId = [zoneinfoReq.zoneId]
    zonelevel = dggrs_provider.get_cells_zone_level(zoneId)[0]
    zoneinfo = dggrs_provider.zonesinfo(zoneId)
    filter_ = 0
    for k, v in collection.items():
        if (v.collection_provider.dggrsId != dggs_info.id):
            converted_zones = dggrs_provider.convert([zoneinfoReq.zoneId], v.collection_provider.dggrsId)
            zoneId = converted_zones.target_zoneIds
            zonelevel = converted_zones.target_res[0]
        params = v.collection_provider.getdata_params
        data = collection_provider[v.collection_provider.providerId].get_data(zoneId, zonelevel, **params)
        filter_ += len(data.zoneIds)
    zoneId = zoneinfoReq.zoneId  # reset the zoneId to original one as string
    if (filter_ > 0):
        dggs_link = '/'.join(str(current_url).split('/')[:-3])
        dggs_link = Link(href=dggs_link, rel='[ogc-rel:dggrs]', title='Link back to /dggs (get list of supported dggs)')
        data_link = Link(href=str(current_url) + '/data', rel='[ogc-rel:dggrs-zone-data]', title='Link to data-retrieval for the zoneId)')
        return_ = {'id': str(zoneId)}
        return_['level'] = zoneinfo.zone_level
        return_['links'] = [data_link, dggs_link]
        return_['shapeType'] = zoneinfo.shapeType
        return_['crs'] = dggs_info.crs
        return_['centroid'] = zoneinfo.centroids[0]
        return_['bbox'] = zoneinfo.bbox[0]
        return_['geometry'] = zoneinfo.geometry[0]
        return_['areaMetersSquare'] = zoneinfo.areaMetersSquare
        logger.debug(f'{__name__} query zone info {zoneinfoReq.dggrsId}, zone id: {zoneinfoReq.zoneId}, zoneinfo: {pprint(return_)}')
        return ZoneInfoResponse(**return_)
    return None
