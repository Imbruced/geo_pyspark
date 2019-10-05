from unittest import TestCase

from pyspark.sql import SparkSession

from geo_pyspark.data import csv_polygon_input_location, csv_point_input_location, overlap_polygon_input_location
from geo_pyspark.register import GeoSparkRegistrator


spark = SparkSession.builder. \
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestPredicateJoin(TestCase):

    def test_st_contains_in_join(self):
        polygon_csv_df = spark.read.format("csv").\
                option("delimiter", ",").\
                option("header", "false").load(
            csv_polygon_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()

        polygon_df = spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_point_input_location
        )
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()

        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = spark.sql(
            "select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 1000

    def test_st_intersects_in_a_join(self):
        polygon_csv_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_polygon_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()

        polygon_df = spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_point_input_location
        )
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()

        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = spark.sql(
            "select * from polygondf, pointdf where ST_Intersects(polygondf.polygonshape,pointdf.pointshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 1000

    def test_st_touches_in_a_join(self):
        polygon_csv_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(csv_polygon_input_location)
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()
        polygon_df = spark.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(csv_point_input_location)
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()
        point_df = spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = spark.sql("select * from polygondf, pointdf where ST_Touches(polygondf.polygonshape,pointdf.pointshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 1000

    def test_st_within_in_a_join(self):
        polygon_csv_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_polygon_input_location)
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()

        polygon_df = spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            csv_point_input_location)
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()

        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = spark.sql(
            "select * from polygondf, pointdf where ST_Within(pointdf.pointshape, polygondf.polygonshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 1000

    def test_st_overlaps_in_a_join(self):
        polygon_csv_df = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
                csv_polygon_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")

        polygon_df = spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")

        polygon_csv_overlap_df = spark.read.format("csv").option("delimiter", ",").option("header", "false").load(
            overlap_polygon_input_location)
        polygon_csv_overlap_df.createOrReplaceTempView("polygonoverlaptable")

        polygon_overlap_df = spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygonoverlaptable._c0 as Decimal(24,20)),cast(polygonoverlaptable._c1 as Decimal(24,20)), cast(polygonoverlaptable._c2 as Decimal(24,20)), cast(polygonoverlaptable._c3 as Decimal(24,20))) as polygonshape from polygonoverlaptable")
        polygon_overlap_df.createOrReplaceTempView("polygonodf")

        range_join_df = spark.sql(
            "select * from polygondf, polygonodf where ST_Overlaps(polygondf.polygonshape, polygonodf.polygonshape)")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 57

    def test_st_crosses_in_a_join(self):
        polygon_csv_df = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_polygon_input_location
        )
        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()

        polygon_df = spark.sql(
            "select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_point_input_location
        )
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()

        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = spark.sql(
            "select * from polygondf, pointdf where ST_Crosses(pointdf.pointshape, polygondf.polygonshape) ")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 1000

    def test_st_distance_radius_in_a_join(self):
        point_csv_df_1 = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_point_input_location
        )
        point_csv_df_1.createOrReplaceTempView("pointtable")
        point_csv_df_1.show()

        point_df_1 = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
        point_df_1.createOrReplaceTempView("pointdf1")
        point_df_1.show()

        point_csv_df_2 = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
            csv_point_input_location)
        point_csv_df_2.createOrReplaceTempView("pointtable")
        point_csv_df_2.show()

        point_df2 = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
        point_df2.createOrReplaceTempView("pointdf2")
        point_df2.show()

        distance_join_df = spark.sql(
            "select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) <= 2")
        distance_join_df.explain()
        distance_join_df.show(10)
        assert distance_join_df.count() == 2998

    def test_st_distance_less_radius_in_a_join(self):
        point_csv_df_1 = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(csv_point_input_location)

        point_csv_df_1.createOrReplaceTempView("pointtable")
        point_csv_df_1.show()

        point_df1 = spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape1 from pointtable")
        point_df1.createOrReplaceTempView("pointdf1")
        point_df1.show()

        point_csv_df2 = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(csv_point_input_location)
        point_csv_df2.createOrReplaceTempView("pointtable")
        point_csv_df2.show()
        point_df2 = spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape2 from pointtable")
        point_df2.createOrReplaceTempView("pointdf2")
        point_df2.show()

        distance_join_df = spark.sql("select * from pointdf1, pointdf2 where ST_Distance(pointdf1.pointshape1,pointdf2.pointshape2) < 2")
        distance_join_df.explain()
        distance_join_df.show(10)
        assert distance_join_df.count() == 2998

    def test_st_contains_in_a_range_and_join(self):
        polygon_csv_df = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_polygon_input_location)

        polygon_csv_df.createOrReplaceTempView("polygontable")
        polygon_csv_df.show()
        polygon_df = spark.sql("select ST_PolygonFromEnvelope(cast(polygontable._c0 as Decimal(24,20)),cast(polygontable._c1 as Decimal(24,20)), cast(polygontable._c2 as Decimal(24,20)), cast(polygontable._c3 as Decimal(24,20))) as polygonshape from polygontable")
        polygon_df.createOrReplaceTempView("polygondf")
        polygon_df.show()

        point_csv_df = spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_point_input_location)
        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show()
        point_df = spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as pointshape from pointtable")
        point_df.createOrReplaceTempView("pointdf")
        point_df.show()

        range_join_df = spark.sql("select * from polygondf, pointdf where ST_Contains(polygondf.polygonshape,pointdf.pointshape) " +
        "and ST_Contains(ST_PolygonFromEnvelope(1.0,101.0,501.0,601.0), polygondf.polygonshape)")

        range_join_df.explain()
        range_join_df.show(3)
        assert range_join_df.count() == 500
