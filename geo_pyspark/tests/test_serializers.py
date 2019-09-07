from unittest import TestCase
from pyspark.sql import SparkSession

from geo_pyspark.utils.serde import KryoSerializer, GeoSparkKryoRegistrator
from geo_pyspark.register import GeoSparkRegistrator


class TestGeoSparkKryo(TestCase):

    def test_kryo_registrator(self):
        spark = SparkSession.builder.\
            master("local[2]").\
            config("spark.serializer", KryoSerializer.getName).\
            config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName).\
            getOrCreate()
