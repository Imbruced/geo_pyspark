from abc import ABC

from pyspark import PickleSerializer

from geo_pyspark.utils.binary_parser import BinaryParser
from geo_pyspark.utils.decorators import classproperty
from geo_pyspark.utils.spatial_rdd_parser import SpatialRDDParserData, SpatialPairRDDParserData

PARSERS = {
    0: SpatialRDDParserData(),
    1: SpatialRDDParserData(),
    2: SpatialPairRDDParserData(),


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


class GeoSparkPickler(PickleSerializer):

    def __init__(self):
        super().__init__()

    def loads(self, obj, encoding="bytes"):
        binary_parser = BinaryParser(obj)
        spatial_parser_number = binary_parser.read_int()
        spatial_parser = self.get_parser(spatial_parser_number)
        parsed_row = spatial_parser.deserialize(binary_parser)

        return parsed_row

    def dumps(self, obj):
        raise NotImplementedError()

    def get_parser(self, number: int):
        return PARSERS[number]
