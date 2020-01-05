from abc import ABC
from typing import List, Any

import attr
from shapely.geometry.base import BaseGeometry

from geo_pyspark.utils.binary_parser import BinaryParser, BinaryBuffer


@attr.s
class GeoData:
    geom = attr.ib(type=BaseGeometry)
    userData = attr.ib(type=str)

    def getUserData(self):
        return self.userData


@attr.s
class GeometryParser(ABC):

    @property
    def name(self):
        raise NotImplementedError

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: BinaryBuffer):
        raise NotImplementedError("Parser has to implement serialize method")

    @classmethod
    def deserialize(cls, bin_parser: BinaryParser) -> BaseGeometry:
        raise NotImplementedError("Parser has to implement deserialize method")


@attr.s
class AbstractSpatialRDDParser(ABC):

    @classmethod
    def serialize(cls, obj: List[Any], binary_buffer: BinaryBuffer) -> bytearray:
        raise NotImplemented()

    @classmethod
    def deserialize(cls, bin_parser: BinaryParser) -> BaseGeometry:
        raise NotImplementedError("Parser has to implement deserialize method")

    @classmethod
    def _deserialize_geom(cls, bin_parser: BinaryParser) -> GeoData:
        from geo_pyspark.sql.geometry import GeometryFactory

        user_data_length = bin_parser.read_int()
        geom = GeometryFactory.geometry_from_bytes(bin_parser)
        if user_data_length > 0:
            user_data = bin_parser.read_string(user_data_length)
            geo_data = GeoData(geom=geom, userData=user_data)

        else:
            geo_data = GeoData(geom=geom, userData="")
        return geo_data
