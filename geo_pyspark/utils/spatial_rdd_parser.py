import attr
from shapely.geometry.base import BaseGeometry

from geo_pyspark.sql.geometry import GeometryFactory
from geo_pyspark.utils.abstract_parser import AbstractSpatialRDDParser
from geo_pyspark.utils.binary_parser import BinaryParser, BinaryBuffer


@attr.s
class SpatialPairRDDParserNonUserData(AbstractSpatialRDDParser):
    name = "SpatialRDDParser"

    @classmethod
    def deserialize(cls, bin_parser: BinaryParser):
        user_data_length = bin_parser.read_int()
        left_geom = GeometryFactory.geometry_from_bytes(bin_parser)
        if user_data_length > 0:
            user_data = bin_parser.read_string(user_data_length)
            left_geom = [left_geom, *user_data.split("\t")]
        else:
            bin_parser.read_byte()

        geometry_numbers = bin_parser.read_int()

        right_geom = []

        if geometry_numbers > 1:
            print("s")

        for right_geometry_number in range(geometry_numbers):
            user_data_length = bin_parser.read_int()
            right_geom_data = GeometryFactory.geometry_from_bytes(bin_parser)
            if user_data_length > 0:
                user_data = bin_parser.read_string(user_data_length)
                right_geom.append([right_geom_data, *user_data.split("\t")])
            else:
                bin_parser.read_byte()
                right_geom.append(right_geom_data)


        deserialized_data = [left_geom, right_geom]

        return deserialized_data

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: BinaryBuffer):
        raise NotImplementedError("Currently this operation is not supported")


@attr.s
class SpatialPairRDDParserUserData(AbstractSpatialRDDParser):
    pass



@attr.s
class SpatialRDDParser(AbstractSpatialRDDParser):

    @classmethod
    def deserialize(cls, bin_parser: BinaryParser):
        left_geom = GeometryFactory.geometry_from_bytes(bin_parser)

        return left_geom

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: BinaryBuffer):
        raise NotImplementedError("Currently this operation is not supported")


