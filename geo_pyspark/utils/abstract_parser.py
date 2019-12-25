from abc import ABC
from typing import List, Any

import attr
from shapely.geometry.base import BaseGeometry

from geo_pyspark.utils.binary_parser import BinaryParser, BinaryBuffer


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
