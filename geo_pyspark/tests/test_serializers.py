from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql import types as t
from shapely.geometry import Point

from geo_pyspark.utils.serde import KryoSerializer, GeoSparkKryoRegistrator
from geo_pyspark.register import GeoSparkRegistrator
from geo_pyspark.sql.types import GeometryType


spark = SparkSession.builder.\
        getOrCreate()


class TestGeoSparkKryo(TestCase):

    def test_kryo_registrator(self):
        spark = SparkSession.builder.\
            master("local[2]").\
            config("spark.serializer", KryoSerializer.getName).\
            config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName).\
            getOrCreate()

    def test_serialize_point(self):
        data = [[1, "name", Point(21.0, 52.0)]]
        schema = t.StructType(
            [
                t.StructField("id", t.IntegerType(), False),
                t.StructField("name", t.StringType(), False),
                t.StructField("location", GeometryType(), False)
            ]
        )
        spark.createDataFrame(
            data,
            schema
        ).show()
