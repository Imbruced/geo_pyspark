from abc import ABC
from enum import Enum

import attr
from shapely.geometry.base import BaseGeometry

from exceptions import GeometryUnavailableException
from utils.abstract_parser import GeometryParser
from utils.binary_parser import BinaryParser
from utils.decorators import classproperty
from utils.parsers import UndefinedParser, PointParser, PolyLineParser, PolygonParser, MultiPointParser


class GeomEnum(Enum):
    undefined = 0
    point = 1
    polyline = 3
    polygon = 5
    multipoint = 8

    @classmethod
    def has_value(cls, value):
        return value in cls._value2member_map_

    @classmethod
    def get_name(cls, value):
        return cls._value2member_map_[value].name


@attr.s
class GeometryFactory:

    @classmethod
    def geometry_from_bytes(cls, bytes: bytearray) -> BaseGeometry:
        bin_parser = BinaryParser(bytes)
        g_type = bin_parser.read_byte()
        gm_type = bin_parser.read_byte()
        if GeomEnum.has_value(gm_type):
            name = GeomEnum.get_name(gm_type)
            parser: GeometryParser = cls.parsers[name]
            geom = parser.deserialize(bin_parser)
            return geom
        else:
            raise GeometryUnavailableException(f"Can not deserialize object")

    @classproperty
    def parsers(self):
        geom_cls = dict(
            undefined=UndefinedParser,
            point=PointParser,
            polyline=PolyLineParser,
            polygon=PolygonParser,
            multipoint=MultiPointParser
        )
        return geom_cls
