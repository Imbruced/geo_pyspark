from typing import Union

import attr
from shapely.geometry import Point
from shapely.geometry import Polygon
from shapely.geometry import MultiPolygon
from shapely.geometry import LineString
from shapely.geometry import MultiLineString
from shapely.geometry import MultiPoint
from shapely.geometry.base import BaseGeometry


from geo_pyspark.utils.abstract_parser import GeometryParser
from geo_pyspark.utils.binary_parser import BinaryParser


@attr.s
class OffsetsReader:

    @staticmethod
    def read_offsets(parser, num_parts, max_offset):
        offsets = []
        for i in range(num_parts):
            offsets.append(parser.read_int())
        offsets.append(max_offset)
        return offsets


@attr.s
class PointParser(GeometryParser):
    name = "Point"

    @classmethod
    def serialize(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Point:
        x = parser.read_double()
        y = parser.read_double()
        return Point(x, y)


@attr.s
class UndefinedParser(GeometryParser):
    name = "Undefined"

    @classmethod
    def serialize(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> BaseGeometry:
        raise NotImplementedError()


@attr.s
class PolyLineParser(GeometryParser):
    name = "Polyline"

    @classmethod
    def serialize(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Union[LineString, MultiLineString]:
        raise NotImplementedError()


def read_coordinates(parser: BinaryParser, read_scale: int):
    coordinates = []
    for i in range(read_scale):
        coordinates.append((parser.read_double(), parser.read_double()))
    return coordinates


@attr.s
class PolygonParser(GeometryParser):
    name = "Polygon"

    @classmethod
    def serialize(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, parser: BinaryParser) -> Union[Polygon, MultiPolygon]:
        """TODO exception handling for shapely constructors"""
        for x in range(4):
            parser.read_double()
        num_rings = parser.read_int()
        num_points = parser.read_int()
        offsets = OffsetsReader.read_offsets(parser, num_parts=num_rings, max_offset=num_points)
        polygons = []
        for i in range(num_rings):
            read_scale = offsets[i + 1] - offsets[i]
            cs_ring = read_coordinates(parser, read_scale)
            polygons.append(Polygon(cs_ring))
        return MultiPolygon(polygons)


@attr.s
class MultiPointParser(GeometryParser):
    name = "MultiPoint"

    @classmethod
    def serialize(cls):
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, bytes: bytearray) -> MultiPoint:
        raise NotImplementedError()
