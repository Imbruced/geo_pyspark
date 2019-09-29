from unittest import TestCase

from pyspark.sql.types import IntegerType

from geo_pyspark.register import GeoSparkRegistrator
from geo_pyspark.sql.types import GeometryType
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString
from pyspark.sql import types as t, SparkSession

spark = SparkSession.builder.\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestsSerializers(TestCase):

    def test_point_serializer(self):
        data = [
            [1, Point(21.0, 56.0), Point(21.0, 59.0)]

        ]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom_from", GeometryType(), True),
                t.StructField("geom_to", GeometryType(), True)
            ]
        )
        spark.createDataFrame(
            data,
            schema
        ).createOrReplaceTempView("points")

        distance = spark.sql(
            "select st_distance(geom_from, geom_to) from points"
        ).collect()[0][0]
        self.assertEqual(distance, 3.0)

    def test_multipoint_serializer(self):

        multipoint = MultiPoint([
                [21.0, 56.0],
                [21.0, 57.0]
             ])
        data = [
            [1, multipoint]
        ]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom", GeometryType(), True)
            ]
        )
        m_point_out = spark.createDataFrame(
            data,
            schema
        ).collect()[0][1]

        self.assertEqual(m_point_out, multipoint)

    def test_linestring_serialization(self):
        linestring = LineString([(0.0, 1.0), (1, 1), (12.0, 1.0)])
        data = [
            [1, linestring]
        ]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom", GeometryType(), True)
            ]
        )

        spark.createDataFrame(
            data,
            schema
        ).createOrReplaceTempView("line")

        length = spark.sql("select st_length(geom) from line").collect()[0][0]
        self.assertEqual(length, 12.0)

    def test_multilinestring_serialization(self):
        multilinestring = MultiLineString([[[0, 1], [1, 1]], [[2, 2], [3, 2]]])
        data = [
            [1, multilinestring]
        ]

        schema = t.StructType(
            [
                t.StructField("id", IntegerType(), True),
                t.StructField("geom", GeometryType(), True)
            ]
        )

        spark.createDataFrame(
            data,
            schema
        ).createOrReplaceTempView("multilinestring")

        spark.sql(
            "select geom from multilinestring"
        ).show(1, False)

        length = spark.sql("select st_length(geom) from multilinestring").collect()[0][0]
        self.assertEqual(length, 2.0)

