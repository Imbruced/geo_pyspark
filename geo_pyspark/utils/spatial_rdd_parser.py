import attr
from shapely.geometry.base import BaseGeometry

from geo_pyspark.sql.geometry import GeometryFactory
from geo_pyspark.utils.abstract_parser import GeometryParser
from geo_pyspark.utils.binary_parser import BinaryParser, BinaryBuffer


@attr.s
class SpatialRDDParser(GeometryParser):
    name = "SpatialRDDParser"

    @classmethod
    def deserialize(cls, bin_parser: BinaryParser):
        is_pair_rdd = bin_parser.read_int()
        left_geom = GeometryFactory.geometry_from_bytes(bin_parser)

        if is_pair_rdd:
            if bin_parser.read_int():
                geometry_numbers = bin_parser.read_double()
                right_geom = []

                for right_geometry_number in range(geometry_numbers):
                    right_geom.append(GeometryFactory.geometry_from_bytes(bin_parser))
                    bin_parser.read_double()
            else:
                right_geom = GeometryFactory.geometry_from_bytes(bin_parser)

            deserialized_data = [left_geom, right_geom]
        else:
            deserialized_data = left_geom

        return deserialized_data

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: BinaryBuffer):
        raise NotImplementedError()