import attr
from shapely.geometry.base import BaseGeometry

from geo_pyspark.utils.abstract_parser import AbstractSpatialRDDParser
from geo_pyspark.utils.binary_parser import BinaryParser, BinaryBuffer


@attr.s
class SpatialPairRDDParserData(AbstractSpatialRDDParser):
    name = "SpatialPairRDDParserData"

    @classmethod
    def deserialize(cls, bin_parser: BinaryParser):
        left_geom_data = cls._deserialize_geom(bin_parser)

        _ = bin_parser.read_int()

        right_geom_data = cls._deserialize_geom(bin_parser)

        deserialized_data = [left_geom_data, right_geom_data]

        return deserialized_data

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: BinaryBuffer):
        raise NotImplementedError("Currently this operation is not supported")


@attr.s
class SpatialRDDParserData(AbstractSpatialRDDParser):
    name = "SpatialRDDParser"

    @classmethod
    def deserialize(cls, bin_parser: BinaryParser):
        left_geom_data = cls._deserialize_geom(bin_parser)

        geometry_numbers = bin_parser.read_int()

        right_geoms = []

        for right_geometry_number in range(geometry_numbers):
            right_geom_data = cls._deserialize_geom(bin_parser)
            right_geoms.append(right_geom_data)

        deserialized_data = [left_geom_data, right_geoms] if right_geoms else left_geom_data

        return deserialized_data

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: BinaryBuffer):
        raise NotImplementedError("Currently this operation is not supported")
