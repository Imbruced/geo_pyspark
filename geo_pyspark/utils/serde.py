from abc import ABC

from pyspark import PickleSerializer

from geo_pyspark.utils.decorators import classproperty


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

    def loads(self, obj, encoding="bytes"):
        return [el for el in obj]

    def dumps(self, obj):
        return super().dumps(obj)