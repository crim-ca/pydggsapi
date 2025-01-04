# here should be DGGRID related functions and methods
# DGGRID ISEA7H resolutions
from pydggsapi.dependencies.dggrs_providers.AbstractDGGRS import VirtualAbstractDGGRS
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint
from pydggsapi.schemas.api.dggsproviders import DGGRSProviderZoneInfoReturn, DGGRSProviderZonesListReturn, DGGRSProviderGetRelativeZoneLevelsReturn, DGGRSProviderZonesElement

import os
import tempfile
import logging
from typing import Union, List, Any
from dggrid4py import DGGRIDv7
import shapely
import h3
import geopandas as gpd
from shapely.geometry import box


class VH3_IGEO7(VirtualAbstractDGGRS):

    def __init__(self):
        executable = os.environ['DGGRID_PATH']
        working_dir = tempfile.mkdtemp()
        super().__init__(h3, DGGRIDv7(executable=executable, working_dir=working_dir))

    def forward():
        pass

    def backward(self):
        pass

    def get_cells_zone_level(self, cellIds: list) -> List[int]:
        zoneslevel = []
        try:
            for c in cellIds:
                zoneslevel.append(self.virtualdggrs.get_resolution(c))
            return zoneslevel
        except Exception as e:
            logging.error(f'{__name__} zone id {cellIds} dggrid get zone level failed: {e}')
            raise Exception(f'{__name__} zone id {cellIds} dggrid get zone level failed: {e}')

    def get_relative_zonelevels(self, cellId: Any, base_level: int, zone_levels: List[int],
                                geometry: str) -> DGGRSProviderGetRelativeZoneLevelsReturn:
        raise NotImplementedError

    def zoneslist(self, bbox: Union[box, None], zone_level: int, parent_zone: Union[str, int, None],
                  returngeometry: str, compact=True) -> DGGRSProviderZonesListReturn:
        if (bbox is not None):
            try:
                zoneIds = self.virtualdggrs.h3shape_to_cells(self.virtualdggrs.geo_to_h3shape(bbox), zone_level)
                geometry = [self._cell_to_shapely(z, returngeometry) for z in zoneIds]
                hex_gdf = gpd.GeoDataFrame({'zoneIds': zoneIds}, geometry=geometry, crs='wgs84').set_index('zoneIds')
            except Exception as e:
                logging.error(f'{__name__} query zones list, bbox: {bbox} dggrid convert failed :{e}')
                raise Exception(f"{__name__} query zones list, bbox: {bbox} dggrid convert failed {e}")
            logging.info(f'{__name__} query zones list, number of hexagons: {len(hex_gdf)}')
        if (parent_zone is not None):
            try:
                children_zoneIds = self.virtualdggrs.cell_to_children(parent_zone, zone_level)
                children_geometry = [self._cell_to_shapely(z, returngeometry) for z in children_zoneIds]
                children_hex_gdf = gpd.GeoDataFrame({'zoneIds': children_zoneIds}, geometry=children_geometry, crs='wgs84').set_index('zoneIds')
                hex_gdf = hex_gdf.join(children_hex_gdf, how='inner', rsuffix='_p') if (bbox is not None) else children_hex_gdf
            except Exception as e:
                logging.error(f'{__name__} query zones list, parent_zone: {parent_zone} get children failed {e}')
                raise Exception(f'parent_zone: {parent_zone} get children failed {e}')
        if (len(hex_gdf) == 0):
            raise Exception(f"{__name__} Parent zone {parent_zone} is not with in bbox: {bbox} at zone level {zone_level}")
        if (compact):
            compactIds = self.virtualdggrs.compact_cells(hex_gdf.index.values)
            geometry = [self._cell_to_shapely(z, returngeometry) for z in compactIds]
            hex_gdf = gpd.GeoDataFrame({'zoneIds': compactIds}, geometry=geometry, crs='wgs84').set_index('zoneIds')
            logging.info(f'{__name__} query zones list, compact : {len(hex_gdf)}')
        returnedAreaMetersSquare = sum([self.virtualdggrs.cell_area(z, 'm^2') for z in hex_gdf.index.values])
        geotype = GeoJSONPolygon if (returngeometry == 'zone-region') else GeoJSONPoint
        geometry = [geotype(**eval(shapely.to_geojson(g))) for g in hex_gdf['geometry'].values.tolist()]
        hex_gdf.reset_index(inplace=True)
        return DGGRSProviderZonesListReturn(**{'zones': hex_gdf['zoneIds'].values.astype(str).tolist(),
                                               'geometry': geometry,
                                               'returnedAreaMetersSquare': returnedAreaMetersSquare})

    def zonesinfo(self, cellIds: list) -> DGGRSProviderZoneInfoReturn:
        centroid = []
        hex_geometry = []
        total_area = []
        try:
            zone_level = self.get_cells_zone_level([cellIds[0]])[0]
            for c in cellIds:
                centroid.append(self._cell_to_shapely(c, 'zone-centroid'))
                hex_geometry.append(self._cell_to_shapely(c, 'zone-region'))
                total_area.append(self.virtualdggrs.cell_area(c))
        except Exception as e:
            logging.error(f'{__name__} zone id {cellIds} dggrid convert failed: {e}')
            raise Exception(f'{__name__} zone id {cellIds} dggrid convert failed: {e}')
        geometry, bbox, centroids = [], [], []
        for g in hex_geometry:
            geometry.append(GeoJSONPolygon(**eval(shapely.to_geojson(g))))
            bbox.append(list(g.bounds))
        for c in centroid:
            centroids.append(GeoJSONPoint(**eval(shapely.to_geojson(c))))
        return DGGRSProviderZoneInfoReturn(**{'zone_level': zone_level, 'shapeType': 'hexagon',
                                              'centroids': centroids, 'geometry': geometry, 'bbox': bbox,
                                              'areaMetersSquare': (sum(total_area) / len(cellIds)) * 1000000})

    # source : https://medium.com/@jesse.b.nestler/how-to-convert-h3-cell-boundaries-to-shapely-polygons-in-python-f7558add2f63
    def _cell_to_shapely(self, cellid, geometry):
        method = self.virtualdggrs.cell_to_boundary if (geometry == 'zone-region') else self.virtualdggrs.cell_to_latlng
        GEO = shapely.Polygon if (geometry == 'zone-region') else shapely.Point
        points = method(cellid)
        points = [points] if (geometry != 'zone-region') else points
        points = tuple(p[::-1] for p in points)
        return GEO(points)



