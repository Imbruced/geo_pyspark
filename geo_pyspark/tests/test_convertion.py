import os
from unittest import TestCase

from pyspark.sql import SparkSession

from geo_pyspark.register import GeoSparkRegistrator

spark = SparkSession.builder.\
        config("--master", "local").\
        getOrCreate()


class TestGeometryConvert(TestCase):

    def test_register_functions(self):
        os.environ["JAVA_HOME"] = "C:\\Program Files\\AdoptOpenJDK\\jdk-8.0.212.04-hotspot"
        GeoSparkRegistrator.registerAll(spark)
        df = spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""")
        df.show()

    def test_geometry_factory(self):
        pass

    def test_collect(self):
        os.environ["JAVA_HOME"] = "C:\\Program Files\\AdoptOpenJDK\\jdk-8.0.212.04-hotspot"
        GeoSparkRegistrator.registerAll(spark)
        df = spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""")
        df.collect()
