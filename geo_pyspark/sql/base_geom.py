from abc import ABC

from shapely.geometry import Point as ShapelyPoint, LinearRing as ShapelyLinearRing
from shapely.geometry import Polygon as ShapelyPolygon
from shapely.geometry import MultiPolygon as ShapelyMultiPolygon
from shapely.geometry import LineString as ShapelyLineString
from shapely.geometry import MultiLineString as ShapelyMultiLineString
from shapely.geometry import MultiPoint as ShapelyMultiPoint
import attr

from geo_pyspark.sql.types import GeometryType
from geo_pyspark.utils.types import numeric


class Point(ShapelyPoint):
    __UDT__ = GeometryType()

    def __init__(self, x: numeric, y: numeric):
        super().__init__(x, y)


class MultiPoint(ShapelyMultiPoint):
    pass

class LineString(ShapelyLineString):
    __UDT__ = GeometryType()

    def __init__(self, x: numeric, y: numeric):
        super().__init__(x, y)


class MultiLineString(ShapelyMultiLineString):
    __UDT__ = GeometryType()
    pass


class MultiLineString(ShapelyMultiLineString):
    __UDT__ = GeometryType()
    pass