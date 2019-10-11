import os
from unittest import TestCase

from pyspark.sql.types import IntegerType
import geopandas as gpd

from geo_pyspark.data import data_path
from geo_pyspark.register import GeoSparkRegistrator, upload_jars
from geo_pyspark.sql.types import GeometryType
from shapely.geometry import Point, MultiPoint, LineString, MultiLineString, Polygon, MultiPolygon
from pyspark.sql import types as t, SparkSession

upload_jars()

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

        length = spark.sql("select st_length(geom) from multilinestring").collect()[0][0]
        self.assertEqual(length, 2.0)

    def test_polygon_serialization(self):
        ext = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
        int = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]

        polygon = Polygon(ext, [int])

        data = [
            [1, polygon]
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
        ).createOrReplaceTempView("polygon")

        length = spark.sql("select st_area(geom) from polygon").collect()[0][0]
        self.assertEqual(length, 3.75)

    def test_geopandas_convertion(self):
        gdf = gpd.read_file(os.path.join(data_path, "gis_osm_pois_free_1.shp"))
        print(spark.createDataFrame(
            gdf
        ).toPandas())

    def test_multipolygon_serialization(self):
        ext = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
        int = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]

        polygons = [
            Polygon(ext, [int]),
            Polygon([[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]])
        ]
        multipolygon = MultiPolygon(polygons)

        data = [
            [1, multipolygon]
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
        ).createOrReplaceTempView("polygon")
        length = spark.sql("select st_area(geom) from polygon").collect()[0][0]
        self.assertEqual(length, 4.75)
