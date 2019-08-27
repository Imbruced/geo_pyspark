from typing import Union

import attr
from shapely.geometry import Point
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon
from shapely.geometry import LineString
from shapely.geometry import MultiLineString
from shapely.geometry import MultiPoint
from shapely.geometry.base import BaseGeometry

from utils.abstract_parser import GeometryParser
from utils.binary_parser import BinaryParser


@attr.s
class PointParser(GeometryParser):
    name = "Point"

    @classmethod
    def serialize(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, bytes: bytearray) -> Point:
        no_neg = cls.remove_negatives(bytes)
        x = BinaryParser.read_double(bytearray(no_neg[2: 10]))
        y = BinaryParser.read_double(bytearray(no_neg[10: 18]))
        return Point(x[0], y[0])


@attr.s
class UndefinedParser(GeometryParser):
    name = "Undefined"

    @classmethod
    def serialize(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, bytes: bytearray) -> BaseGeometry:
        raise NotImplementedError()


@attr.s
class PolyLineParser(GeometryParser):
    name = "Polyline"

    @classmethod
    def serialize(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, bytes: bytearray) -> Union[LineString, MultiLineString]:
        raise NotImplementedError()


@attr.s
class PolygonParser(GeometryParser):
    name = "Polygon"

    @classmethod
    def serialize(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, bytes: bytearray) -> Union[Polygon, MultiPolygon]:
        raise NotImplementedError()


@attr.s
class MultiPointParser(GeometryParser):
    name = "MultiPoint"

    @classmethod
    def serialize(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, bytes: bytearray) -> MultiPoint:
        raise NotImplementedError()
