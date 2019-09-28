from unittest import TestCase

from pyspark.sql.types import IntegerType

from geo_pyspark.register import GeoSparkRegistrator
from geo_pyspark.sql.types import GeometryType
from shapely.geometry import Point
from pyspark.sql import types as t, SparkSession

spark = SparkSession.builder.\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestsSerializers(TestCase):

    def test_point_serializer(self):
        data = [[1, Point(21.0, 56.0)]]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom", GeometryType(), True)
            ]
        )

        spark.createDataFrame(
            data,
            schema
        ).show()
