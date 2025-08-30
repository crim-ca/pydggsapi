# here should be DGGRID related functions and methods
# DGGRID ISEA7H resolutions

from pydggsapi.dependencies.dggrs_providers.abstract_dggrs_provider import AbstractDGGRSProvider
from pydggsapi.schemas.common_geojson import GeoJSONPolygon, GeoJSONPoint
from pydggsapi.schemas.api.dggrs_providers import DGGRSProviderZoneInfoReturn, DGGRSProviderZonesListReturn
from pydggsapi.schemas.api.dggrs_providers import DGGRSProviderGetRelativeZoneLevelsReturn, DGGRSProviderZonesElement

import shapely
import logging
from dggal import Application, pydggal_setup, ISEA3H, IVEA3H, rHEALPix, printLn, printx, CRS, ogc, epsg, GeoExtent, Array
from typing import Union, List

logger = logging.getLogger()

# upper case
supported_grids = {'ISEA3H': ISEA3H(),
                   'IVEA3H': IVEA3H(),
                   'RHEALPIX': rHEALPix()}


# helper function to generate geometry geojson of a zoneId
def generateZoneGeometry(dggrs, zone, crs, centroids: bool, fc: bool):
    t = "   " if fc else ""
    printLn(t, '   "geometry" : {')
    printLn(t, '      "type" : "', 'Point' if centroids else 'Polygon', '",')
    printx(t, '      "coordinates" : [')

    if not crs or crs == CRS(ogc, 84) or crs == CRS(epsg, 4326):
        if centroids:
            centroid = dggrs.getZoneWGS84Centroid(zone)
            printx(" ", centroid.lon, ", ", centroid.lat)
        else:
            vertices = dggrs.getZoneRefinedWGS84Vertices(zone, 0)
            if vertices:
                count = vertices.count
                printLn("")
                printx(t, "         [ ")
                for i in range(count):
                    printx(", " if i else "", "[", vertices[i].lon, ", ", vertices[i].lat, "]")
                printx(", " if count else "", "[", vertices[0].lon, ", ", vertices[0].lat, "]")
                printLn(" ]")
            printx(t, "     ")
    else:
        if centroids:
            centroid = dggrs.getZoneCRSCentroid(zone, crs)
            printx(" ", centroid.x, ", ", centroid.y)
        else:
            vertices = dggrs.getZoneRefinedCRSVertices(zone, crs, 0)
            if vertices:
                count = vertices.count

            printLn("")
            printLn(t, "         [ ")

            for i in range(count):
                printx(", " if i else "", "[", vertices[i].x, ", ", vertices[i].y, "]")
            printx(", " if count else "", "[", vertices[0].x, ", ", vertices[0].y, "]")
            printLn(" ]")
        printx(t, "     ")
    printLn(" ]")
    printx(t, "   }")


def generateZoneExtent(dggrs, zoneId):
    geoextent = GeoExtent()
    geoextent = dggrs.getZoneWGS84Extent(zoneId, geoextent)
    minx, miny, maxx, maxy = geoextent.ll.lon.value, geoextent.ll.lat.value, geoextent.ur.lon.value, geoextent.ur.lat.value
    extent = shapely.Geometry(((minx, miny), (minx, maxy), (maxx, maxy), (maxx, miny)))
    return extent


class DGGALProvider(AbstractDGGRSProvider):
    def __init__(self, **params):
        self.app = Application(appGlobals=globals())
        pydggal_setup(self.app)
        self.grid_name = params.get('grid', 'ISEA3H').upper()
        self.mygrid = supported_grids(self.grid_name)

    def convert(self, zoneIds: list, targedggrs: type[AbstractDGGRSProvider]):
        pass

    def get_zone_level_by_cls(self, cls_km: float):
        return self.mygrid.getLevelFromMetersPerSubZone(cls_km * 1000, 0)

    def get_cells_zone_level(self, cellIds: List[str]):
        cellId = self.mygrid.getZoneFromTextID(cellIds[0])
        return self.mygrid.getZoneLevel(cellId)

    def get_relative_zonelevels(self, cellId: str, base_level: int, zone_levels: List[int], geometry='zone-region'):
        children = {}
        geometry = geometry.lower()
        geojson = GeoJSONPolygon if (geometry == 'zone-region') else GeoJSONPoint
        cellId = self.mygrid.getZoneFromTextID(cellId)
        for z in zone_levels:
            subzoneIds = self.mygrid.getSubZones(cellId, (z - base_level))
            subzones_geometry = [generateZoneGeometry(self.mygrid, cellId, None, False if (geometry == 'zone-region') else True, False)
                               for cellId in subzoneIds]
            subzoneIds = [self.mygrid.getZoneTextID(id_) for id_ in subzoneIds]
            subzones_geometry = [geojson(**shapely.geometry.mapping(g)) for g in subzones_geometry]
            children[z] = DGGRSProviderZonesElement(**{'zoneIds': subzoneIds,
                                                       'geometry': subzones_geometry})
        return DGGRSProviderGetRelativeZoneLevelsReturn(relative_zonelevels=children)

    def zonesinfo(self, cellIds: List[str]):
        cellIds = [self.mygrid.getZoneFromTextID(cellId) for cellId in cellIds]
        zone_level = self.get_cells_zone_level([cellIds[0]])
        try:
            centroids = [generateZoneGeometry(self.mygrid, cellId, None, True, False)
                         for cellId in cellIds]
            centroids = [GeoJSONPoint(**eval(shapely.to_geojson(c))) for c in centroids]
            hex_vertices = [generateZoneGeometry(self.mygrid, cellId, None, False, False)
                            for cellId in cellIds]
            hex_vertices = [GeoJSONPolygon(**eval(shapely.to_geojson(g))) for g in hex_vertices]
            extents = [generateZoneExtent(self.mygrid, cellId) for cellId in cellIds]
            extents = [GeoJSONPolygon(**eval(shapely.to_geojson(b))) for b in extents]
        except Exception as e:
            logger.error(f'{__name__} zone id {cellIds} convert failed, {e}')
            raise Exception(f'{__name__} zone id {cellIds} convert failed, {e}')
        return DGGRSProviderZoneInfoReturn(**{'zone_level': zone_level, 'shapeType': 'hexagon',
                                              'centroids': centroids, 'geometry': hex_vertices, 'bbox': extents,
                                              'areaMetersSquare': self.mygrid.getRefZoneArea(zone_level)})

    def zoneslist(self, bbox: Union[shapely.box, None], zone_level: int, parent_zone: Union[str, int, None], returngeometry: str, compact=True):
        if (bbox is not None):
            try:
                bbox = shapely.bounds(bbox)
                geoextent = GeoExtent(ll={'lat': bbox[1], 'lon': bbox[0]}, ur={'lat': bbox[3], 'lon': bbox[2]})
                zones_list = self.mygrid.listZones(zone_level, geoextent)
                zones_list = set(self.mygrid.getZoneTextID(z) for z in zones_list)
            except Exception as e:
                logger.error(f'{__name__} query zones list, bbox: {bbox} dggrid convert failed :{e}')
                raise Exception(f"{__name__} query zones list, bbox: {bbox} dggrid convert failed {e}")
            logger.info(f'{__name__} query zones list, number of hexagons: {len(zones_list)}')
        if (parent_zone is not None):
            try:
                parent_zone = self.mygrid.getZoneFromTextID(parent_zone)
                parent_zone_level = self.get_cells_zone_level([parent_zone])
                subzones_list = self.mygrid.getSubZones(parent_zone, (zone_level - parent_zone_level))
                subzones_list = set(z for z in subzones_list)
                zones_list = (zones_list & subzones_list) if (bbox is not None) else subzones_list
            except Exception as e:
                logger.error(f'{__name__} query zones list, parent_zone: {parent_zone} get children failed {e}')
                raise Exception(f'parent_zone: {parent_zone} get children failed {e}')
        if (len(zones_list) == 0):
            raise Exception(f"{__name__} Parent zone {parent_zone} is not with in bbox: {bbox} at zone level {zone_level}")
        if (compact):
            compact_list = Array("<DGGRSZone>")
            [compact_list.add(self.mygrid.getZoneFromTextID(z)) for z in zones_list]
            self.mygrid.compactZones(compact_list)
            zones_list = [self.mygrid.getZoneTextID(z) for z in compact_list]
            logger.info(f'{__name__} query zones list, compact : {len(zones_list)}')
        zones_geometry = [generateZoneGeometry(self.mygrid, z, None, False if (returngeometry == 'zone-region') else True, False) for z in zones_list]
        returnedAreaMetersSquare = sum([self.mygrid.getZoneArea(self.mygrid.getZoneFromTextID(z)) for z in zones_list])
        geotype = GeoJSONPolygon if (returngeometry == 'zone-region') else GeoJSONPoint
        geometry = [geotype(**eval(shapely.to_geojson(g))) for g in zones_geometry]
        return DGGRSProviderZonesListReturn(**{'zones': zones_list,
                                               'geometry': geometry,
                                               'returnedAreaMetersSquare': returnedAreaMetersSquare})
