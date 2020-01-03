from abc import ABC

import attr
from pyspark import PickleSerializer

from geo_pyspark.utils.binary_parser import BinaryParser
from geo_pyspark.utils.decorators import classproperty
from geo_pyspark.utils.spatial_rdd_parser import SpatialPairRDDParserNonUserData

PARSERS = {
    0: SpatialPairRDDParserNonUserData(),
    1: SpatialPairRDDParserNonUserData(),
    2: SpatialPairRDDParserNonUserData(),


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

    def loads(self, obj, encoding="bytes"):
        binary_parser = BinaryParser(obj)
        spatial_parser_number = binary_parser.read_int()
        spatial_parser = self.get_parser(spatial_parser_number)
        parsed_row = spatial_parser.deserialize(binary_parser)

        return parsed_row

    def dumps(self, obj):
        return super().dumps(obj)

    def get_parser(self, number: int):
        return PARSERS[number]

