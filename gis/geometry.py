from abc import ABC
from enum import Enum

import attr
from shapely.geometry.base import BaseGeometry

from exceptions import GeometryUnavailableException
from utils.abstract_parser import GeometryParser
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
    def from_number(cls, value: int) -> Geometry:
        if GeomEnum.has_value(value):
            name = GeomEnum.get_name(value)
            geom = cls.geom_cls[name]
            return geom
        else:
            raise GeometryUnavailableException(f"Geometry number {value} is not available")

    @classproperty
    def geom_cls(self):
        geom_cls = dict(
            undefined=Undefined,
            point=Point,
            polyline=PolyLine,
            polygon=Polygon,
            multipoint=MultiPoint
        )
        return geom_cls
