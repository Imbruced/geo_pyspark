import attr
from shapely.geometry.base import BaseGeometry

from geo_pyspark.utils.abstract_parser import AbstractSpatialRDDParser
from geo_pyspark.utils.binary_parser import BinaryParser, BinaryBuffer


@attr.s
class SpatialPairRDDParserNonUserData(AbstractSpatialRDDParser):
    name = "SpatialRDDParser"

    @classmethod
    def deserialize(cls, bin_parser: BinaryParser):
        from geo_pyspark.sql.geometry import GeometryFactory

        user_data_length = bin_parser.read_int()
        left_geom = GeometryFactory.geometry_from_bytes(bin_parser)
        if user_data_length > 0:
            user_data = bin_parser.read_string(user_data_length)
            left_geom_data = GeoData(geom=left_geom, userData=user_data)

        else:
            left_geom_data = GeoData(geom=left_geom, userData="")

        geometry_numbers = bin_parser.read_int()

        right_geoms = []

        for right_geometry_number in range(geometry_numbers):
            user_data_length = bin_parser.read_int()
            right_geom = GeometryFactory.geometry_from_bytes(bin_parser)
            if user_data_length > 0:
                user_data = bin_parser.read_string(user_data_length)
                right_geom_data = GeoData(geom=right_geom, userData=user_data)
            else:
                right_geom_data = GeoData(geom=right_geom, userData="")
            right_geoms.append(right_geom_data)

        deserialized_data = [left_geom_data, right_geoms] if right_geoms else left_geom_data

        return deserialized_data

    @classmethod
    def serialize(cls, obj: BaseGeometry, binary_buffer: BinaryBuffer):
        raise NotImplementedError("Currently this operation is not supported")


@attr.s
class GeoData:
    geom = attr.ib(type=BaseGeometry)
    userData = attr.ib(type=str)

    def getUserData(self):
        return self.userData