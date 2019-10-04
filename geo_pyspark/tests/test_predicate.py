from unittest import TestCase

from pyspark.sql import SparkSession

from geo_pyspark.data import csv_point_input_location, csv_point1_input_location
from geo_pyspark.register import GeoSparkRegistrator

spark = SparkSession.builder. \
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestPredicate(TestCase):

    def test_st_contains(self):
        point_csv_df = spark.read. \
            format("csv"). \
            option("delimiter", ","). \
            option("header", "false").load(
                csv_point_input_location
            )

        point_csv_df.createOrReplaceTempView("pointtable")
        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        point_df.createOrReplaceTempView("pointdf")

        result_df = spark.sql(
            "select * from pointdf where ST_Contains(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)")
        result_df.show()
        assert (result_df.count() == 999)

    def test_st_intersects(self):
        point_csv_df = spark.read. \
            format("csv"). \
            option("delimiter", ","). \
            option("header", "false").load(
                csv_point_input_location
            )

        point_csv_df.createOrReplaceTempView("pointtable")

        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        result_df = spark.sql(
            "select * from pointdf where ST_Intersects(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)")
        result_df.show()
        assert (result_df.count() == 999)

    def test_st_within(self):
        point_csv_df = spark.read. \
            format("csv"). \
            option("delimiter", ","). \
            option("header", "false").load(
                csv_point_input_location
            )

        point_csv_df.createOrReplaceTempView("pointtable")

        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        result_df = spark.sql(
            "select * from pointdf where ST_Within(pointdf.arealandmark, ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0))")
        result_df.show()
        assert (result_df.count() == 999)

    def test_st_equals_for_st_point(self):
        point_df_csv = spark.read.\
            format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
                csv_point1_input_location
            )

        point_df_csv.createOrReplaceTempView("pointtable")

        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as point from pointtable")
        point_df.createOrReplaceTempView("pointdf")

        equal_df = spark.sql("select * from pointdf where ST_Equals(pointdf.point, ST_Point(100.1, 200.1)) ")
        equal_df.show()

        assert (equal_df.count() == 5, f"Expected 5 value but got ${equal_df.count()}")
