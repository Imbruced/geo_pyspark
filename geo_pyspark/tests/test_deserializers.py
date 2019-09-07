import os
from unittest import TestCase

from pyspark.sql import SparkSession
from shapely.geometry import MultiPoint, Point

from geo_pyspark.data import data_path
from geo_pyspark.register import GeoSparkRegistrator

spark = SparkSession.builder.\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestGeometryConvert(TestCase):

    def test_register_functions(self):
        df = spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""")
        df.show()

    def test_geometry_factory(self):
        pass

    def test_collect(self):

        df = spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""")
        df.collect()

    def test_polygon_deserialization(self):
        geom = spark.read.\
            options(delimiter="|", header=True).\
            csv(os.path.join(data_path, "counties.csv")).\
            limit(1).\
            createOrReplaceTempView("counties")

        geom_area = spark.sql("SELECT st_area(st_geomFromWKT(geom)) as area from counties").collect()[0][0]
        polygon_shapely = spark.sql("SELECT st_geomFromWKT(geom) from counties").collect()[0][0]
        self.assertEqual(geom_area, polygon_shapely.area)

    def test_multipolygon_deserialization(self):
        pass

    def test_point_deserialization(self):
        geom = spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""").collect()[0][0]
        self.assertEqual(
            geom.wkt,
            Point(-6.0, 52.0).wkt
        )

    def test_multipoint_deserialization(self):
        geom = spark.sql("""select st_geomFromWKT('MULTIPOINT(1 2, -2 3)') as geom""").collect()[0][0]

        self.assertEqual(
            geom.wkt,
            MultiPoint([(1, 2), (-2, 3)]).wkt
        )

    def test_linestring_deserialization(self):
        pass

    def test_multilinestring_deserialization(self):
        pass

