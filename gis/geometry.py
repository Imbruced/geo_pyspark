from abc import ABC
from enum import Enum
from plistlib import Dict
import inspect

import attr

from geo_pyspark.exceptions import GeometryUnavailableException
from geo_pyspark.utils.binary_parser import BinaryParser
from geo_pyspark.utils.cls_finder import ClsFinder
from geo_pyspark.utils.decorators import classproperty


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
class Geometry(ABC):

    @classmethod
    def from_bytes(cls, bytes: bytearray) -> 'Geometry':
        raise NotImplementedError


@attr.s
class Point(Geometry):
    x = attr.ib()
    y = attr.ib()

    @classmethod
    def from_bytes(cls, bytes: bytearray) -> 'Geometry':
        from geo_pyspark.utils.point_parser import PointParser
        return PointParser.deserialize(bytes)


@attr.s
class Polygon(Geometry):
    pass


@attr.s
class PolyLine(Geometry):
    pass


@attr.s
class MultiPoint(Geometry):
    pass


@attr.s
class Undefined(Geometry):
    pass


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
