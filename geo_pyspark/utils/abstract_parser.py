from abc import ABC

import attr
from shapely.geometry.base import BaseGeometry

from geo_pyspark.utils.binary_parser import BinaryParser



@attr.s
class GeometryParser(ABC):

    @property
    def name(self):
        raise NotImplementedError

    @classmethod
    def serialize(cls):
        raise NotImplementedError("Parser has to implement serialize method")

    @classmethod
    def deserialize(cls, bin_parser: BinaryParser) -> BaseGeometry:
        raise NotImplementedError("Parser has to implement deserialize method")
