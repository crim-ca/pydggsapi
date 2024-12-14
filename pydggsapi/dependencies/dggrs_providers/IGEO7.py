# here should be DGGRID related functions and methods
# DGGRID ISEA7H resolutions
from pydggsapi.dependencies.dggrs_providers.AbstractDGGRS import AbstractDGGRS
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint
from pydggsapi.schemas.api.dggsproviders import DGGRSProviderZoneInfoReturn, DGGRSProviderZonesListReturn

import os
import tempfile
import logging
from typing import Union
from dggrid4py import DGGRIDv7
import shapely
from shapely.geometry import box
import geopandas as gpd

logging.basicConfig(format='%(asctime)s.%(msecs)03d %(levelname)s {%(module)s} [%(funcName)s] %(message)s',
                    datefmt='%Y-%m-%d,%H:%M:%S', level=logging.INFO)


class IGEO7(AbstractDGGRS):

    def __init__(self):

        executable = os.environ['DGGRID_PATH']
        working_dir = tempfile.mkdtemp()
        self.dggrid_instance = DGGRIDv7(executable=executable, working_dir=working_dir)
        self.data = {
            0: {"Cells": 12, "Area (km^2)": 51006562.1724089, "CLS (km)": 8199.5003701},
            1: {"Cells": 72, "Area (km^2)": 7286651.7389156, "CLS (km)": 3053.2232428},
            2: {"Cells": 492, "Area (km^2)": 1040950.2484165, "CLS (km)": 1151.6430095},
            3: {"Cells": 3432, "Area (km^2)": 148707.1783452, "CLS (km)": 435.1531492},
            4: {"Cells": 24012, "Area (km^2)": 21243.8826207, "CLS (km)": 164.4655799},
            5: {"Cells": 168072, "Area (km^2)": 3034.8403744, "CLS (km)": 62.1617764},
            6: {"Cells": 1176492, "Area (km^2)": 433.5486249, "CLS (km)": 23.4949231},
            7: {"Cells": 8235432, "Area (km^2)": 61.9355178, "CLS (km)": 8.8802451},
            8: {"Cells": 57648012, "Area (km^2)": 8.8479311, "CLS (km)": 3.3564171},
            9: {"Cells": 403536072, "Area (km^2)": 1.2639902, "CLS (km)": 1.2686064},
            10: {"Cells": 2824752492, "Area (km^2)": 0.18057, "CLS (km)": 0.4794882},
            11: {"Cells": 19773267432, "Area (km^2)": 0.0257957, "CLS (km)": 0.1812295},
            12: {"Cells": 138412872012, "Area (km^2)": 0.0036851, "CLS (km)": 0.0684983},
            13: {"Cells": 968890104072, "Area (km^2)": 0.0005264, "CLS (km)": 0.0258899},
            14: {"Cells": 6782230728492, "Area (km^2)": 0.0000752, "CLS (km)": 0.0097855},
            15: {"Cells": 47475615099432, "Area (km^2)": 0.0000107, "CLS (km)": 0.0036986},
        }

    def get(self, zoom):
        # zoom must be integer and between 0 and 15 inclusive
        if not isinstance(zoom, int):
            raise TypeError("zoom must be integer")
        if zoom < 0 or zoom > 15:
            raise ValueError("zoom must be between 0 and 15 inclusive")

        return self.data[zoom]

    def generate_hexgrid(self, bbox, resolution):
        # ISEA7H grid at resolution, for extent of provided WGS84 rectangle into GeoDataFrame
        gdf = self.dggrid_instance.grid_cell_polygons_for_extent('IGEO7', resolution, clip_geom=bbox, output_address_type='Z7_STRING')
        return gdf

    def centroid_from_cellid(self, cellid: list, zone_level):
        gdf = self.dggrid_instance.grid_cell_centroids_from_cellids(cellid, 'IGEO7', zone_level,
                                                                    input_address_type='Z7_STRING', output_address_type='Z7_STRING')
        return gdf

    def hexagon_from_cellid(self, cellid: list, zone_level):
        gdf = self.dggrid_instance.grid_cell_polygons_from_cellids(cellid, 'IGEO7', zone_level,
                                                                   input_address_type='Z7_STRING', output_address_type='Z7_STRING')
        return gdf

    def cellid_from_centroid(self, geodf_points_wgs84, zoomlevel):
        gdf = self.dggrid_instance.cells_for_geo_points(geodf_points_wgs84, True, 'IGEO7', zoomlevel, output_address_type='Z7_STRING')
        return gdf

    def cellids_from_extent(self, clip_geom, zoomlevel):
        gdf = self.dggrid_instance.grid_cellids_for_extent('IGEO7', zoomlevel, clip_geom=clip_geom, output_address_type='Z7_STRING')
        return gdf

    def get_cells_zone_level(self, cellIds):
        try:
            zones_level = self.dggrid_instance.guess_zstr_resolution(cellIds, 'IGEO7', input_address_type='Z7_STRING')
            return zones_level['resolution'].values.tolist()
        except Exception:
            logging.error(f'{__name__} zone id {cellIds} dggrid get zone level failed')
            raise Exception(f'{__name__} zone id {cellIds} dggrid get zone level failed')

    def zonesinfo(self, cellIds: list):
        zone_level = self.dggrid_instance.guess_zstr_resolution(cellIds, 'IGEO7', input_address_type='Z7_STRING')['resolution'][0]
        try:
            centroid = self.centroid_from_cellid(cellIds, zone_level).geometry
            hex_geometry = self.hexagon_from_cellid(cellIds, zone_level).geometry
        except Exception:
            logging.error(f'{__name__} zone id {cellIds} dggrid convert failed')
            raise Exception(f'{__name__} zone id {cellIds} dggrid convert failed')
        geometry, bbox, centroids = [], [], []
        for g in hex_geometry:
            geometry.append(GeoJSONPolygon(**eval(shapely.to_geojson(g))))
            bbox.append(list(g.bounds))
        for c in centroid:
            centroids.append(GeoJSONPoint(**eval(shapely.to_geojson(c))))
        return DGGRSProviderZoneInfoReturn(**{'zone_level': zone_level, 'shapeType': 'hexagon',
                                              'centroids': centroids, 'geometry': geometry, 'bbox': bbox,
                                              'areaMetersSquare': self.data[zone_level]["Area (km^2)"] * 1000000})

    def zoneslist(self, bbox: Union[box, None], zone_level: int, parent_zone: Union[str, int, None], returngeometry: str, compact=True):
        if (bbox is not None):
            try:
                hex_gdf = self.generate_hexgrid(bbox, zone_level)
            except Exception as e:
                logging.error(f'{__name__} query zones list, bbox: {bbox} dggrid convert failed :{e}')
                raise Exception(f"{__name__} query zones list, bbox: {bbox} dggrid convert failed {e}")
            logging.info(f'{__name__} query zones list, number of hexagons: {len(hex_gdf)}')
        if (parent_zone is not None):
            try:
                parent_zone_level = self.get_cells_zone_level([parent_zone])[0]
                parent_hex_gdf = self.hexagon_from_cellid([parent_zone], parent_zone_level)
            except Exception as e:
                logging.error(f'{__name__} query zones list, parent_zone: {parent_zone} get zone level failed {e}')
                raise Exception(f'parent_zone: {parent_zone} get zone level failed {e}')
            childern_hex_gdf = self.dggrid_instance.grid_cell_polygons_from_cellids([parent_zone], 'IGEO7', zone_level,
                                                                                    clip_subset_type='COARSE_CELLS',
                                                                                    clip_cell_res=parent_zone_level,
                                                                                    input_address_type='Z7_STRING',
                                                                                    output_address_type='Z7_STRING')
            hex_gdf = hex_gdf.loc(childern_hex_gdf['name']) if (bbox is not None) else parent_hex_gdf
        if (len(hex_gdf) == 0):
            raise Exception(f"{__name__} Parent zone {parent_zone} is not with in bbox: {bbox} at zone level {zone_level}")
        if (compact):
            compact_zone = parent_hex_gdf['geometry'][0] if (bbox is None) else bbox
            compact_gdf = gpd.GeoDataFrame([0] * len(hex_gdf), geometry=[compact_zone] * len(hex_gdf), crs='wgs84')
            hex_gdf.set_crs('wgs84', inplace=True)
            not_touching = compact_gdf.geometry.contains(hex_gdf.geometry)
            hex_gdf = hex_gdf[not_touching]
            logging.info(f'{__name__} query zones list, compact : {len(hex_gdf)}')
        if (returngeometry != 'zone-region'):
            hex_gdf = self.centroid_from_cellid(hex_gdf['name'].values, zone_level)
        returnedAreaMetersSquare = self.data[zone_level]['Area (km^2)'] * len(hex_gdf) * 1000000
        return DGGRSProviderZonesListReturn(**{'zones': hex_gdf['name'].values.astype(str).tolist(),
                                               'geometry': hex_gdf['geometry'].values.tolist(),
                                               'returnedAreaMetersSquare': returnedAreaMetersSquare})





