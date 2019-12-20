from abc import ABC

import attr
from pyspark import PickleSerializer

from geo_pyspark.utils.abstract_parser import GeometryParser
from geo_pyspark.utils.binary_parser import BinaryParser
from geo_pyspark.utils.decorators import classproperty
from geo_pyspark.utils.spatial_rdd_parser import SpatialRDDParser


PARSERS = {
    0: SpatialRDDParser(),

}


class Serializer(ABC):

    @classproperty
    def getName(self):
        raise NotImplemented()


class KryoSerializer(Serializer):

    @classproperty
    def getName(self):
        return "org.apache.spark.serializer.KryoSerializer"


class GeoSparkKryoRegistrator(Serializer):

    @classproperty
    def getName(self):
        return "org.datasyslab.geospark.serde.GeoSparkKryoRegistrator"


@attr.s
class GeoSparkPickler(PickleSerializer):
    left_carry_input = attr.ib()
    right_carry_input = attr.ib()

    def loads(self, obj, encoding="bytes"):
        byte_array = [el for el in obj]

        binary_parser = BinaryParser(byte_array)
        spatial_parser_number = self.left_carry_input + self.right_carry_input * 2
        spatial_parser = self.get_parser(spatial_parser_number)
        parsed_row = spatial_parser.deserialize(binary_parser)

        return parsed_row

    def dumps(self, obj):
        return super().dumps(obj)

    def get_parser(self, number: int) -> GeometryParser:
        return PARSERS[number]
