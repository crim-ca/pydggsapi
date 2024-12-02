# here should be DGGRID related functions and methods
# DGGRID ISEA7H resolutions
import os
import tempfile
from dggrid4py import DGGRIDv7


class DggridISEA7H:

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

    def find_zoom_by_area_km2(self, area):
        # area must be float and between 0 and 51006562.1724089 inclusive
        if not isinstance(area, float):
            raise TypeError("area must be float")
        if area < 0 or area > 51006562.1724089:
            raise ValueError("area must be between 0 and 51006562.1724089 inclusive")

        for zoom, data in self.data.items():
            if data["Area (km^2)"] <= area:
                return zoom
        return 15

    def find_zoom_by_cls_km(self, cls_km):
        # cls must be float and between 0 and 8199.5003701 inclusive
        if not isinstance(cls_km, float):
            raise TypeError("cls_km must be float")
        if cls_km < 0:
            return 15

        if cls_km > 8199.5003701:
            return 1

        for zoom, data in self.data.items():
            if data["CLS (km)"] <= cls_km:
                return zoom+1
        return 15

    def generate_hexgrid(self, bbox, resolution):
        # ISEA7H grid at resolution, for extent of provided WGS84 rectangle into GeoDataFrame
        gdf1 = self.dggrid_instance.grid_cell_polygons_for_extent('ISEA7H', resolution, clip_geom=bbox)
        gdf1['name'] = gdf1.name.astype('int64')
        return gdf1.set_index('name', drop=True)

    def centroid_from_cellid(self, cellid: list, zoomlevel):
        gdf = self.dggrid_instance.grid_cell_centroids_from_cellids(cellid, 'ISEA7H', zoomlevel)
        return gdf

    def hexagon_from_cellid(self, cellid: list, zoomlevel):
        gdf = self.dggrid_instance.grid_cell_polygons_from_cellids(cellid, 'ISEA7H', zoomlevel)
        return gdf

    def cellid_from_centroid(self, geodf_points_wgs84, zoomlevel):
        gdf = self.dggrid_instance.cells_for_geo_points(geodf_points_wgs84, True, 'ISEA7H', zoomlevel)
        return gdf

    def cellids_from_extent(self, clip_geom, zoomlevel):
        gdf = self.dggrid_instance.grid_cellids_for_extent('ISEA7H', zoomlevel, clip_geom=clip_geom)
        return gdf
