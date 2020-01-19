from copy import copy

from shapely.geometry.base import BaseGeometry


class GeoData:

    def __init__(self, geom: BaseGeometry, userData: str):
        self._geom = geom
        self._userData = userData

    def getUserData(self):
        return self.userData

    def __getstate__(self):
        from geo_pyspark.sql.geometry import GeometryFactory
        attributes = copy(self.__slots__)
        geom = getattr(self, attributes[0])
        return dict(
            geom=bytearray([el if el >= 0 else el + 256 for el in GeometryFactory.to_bytes(geom)]),
            userData=getattr(self, attributes[1])
        )

    def __setstate__(self, attributes):
        from geo_pyspark.sql.geometry import GeometryFactory
        from geo_pyspark.utils.binary_parser import BinaryParser
        bin_parser = BinaryParser(attributes["geom"])
        self._geom = GeometryFactory.geometry_from_bytes(bin_parser)
        self._userData = attributes["userData"]

    @property
    def geom(self):
        return self._geom

    @property
    def userData(self):
        return self._userData

    __slots__ = ("_geom", "_userData")

    def __repr__(self):
        return f"Geometry: {str(self.geom)} userData: {self.userData}"
