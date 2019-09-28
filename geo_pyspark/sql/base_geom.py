from abc import ABC
from typing import Sequence

from shapely.geometry import Point as ShapelyPoint
from shapely.geometry import Polygon as ShapelyPolygon
from shapely.geometry import MultiPolygon as ShapelyMultiPolygon
from shapely.geometry import LineString as ShapelyLineString
from shapely.geometry import MultiLineString as ShapelyMultiLineString
from shapely.geometry import MultiPoint as ShapelyMultiPoint

from geo_pyspark.sql.types import GeometryType
from geo_pyspark.utils.types import numeric


class Point(ShapelyPoint):
    __UDT__ = GeometryType()

    def __init__(self, x: numeric, y: numeric):
        super().__init__(x, y)


class MultiPoint(ShapelyMultiPoint):
    __UDT__ = GeometryType()

    def __init__(self, points: Sequence[Sequence[numeric]]):
        super().__init__(points)


class LineString(ShapelyLineString):
    __UDT__ = GeometryType()

    def __init__(self, coordinates: Sequence[Sequence[numeric]]):
        super().__init__(coordinates)


class MultiLineString(ShapelyMultiLineString):
    __UDT__ = GeometryType()

    def __init__(self, lines):
        super().__init__(lines)

class MultiLineString(ShapelyMultiLineString):
    __UDT__ = GeometryType()
    pass