from pyspark.sql import SparkSession

from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.SpatialRDD import PolygonRDD
from geo_pyspark.register import upload_jars

upload_jars()


spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()


class TestSpatialRDD:

    def test_creating_point_rdd(self):
        point_rdd = PointRDD(
            sparkContext=spark._sc,
            InputLocation="/home/pkocinski001/Desktop/projects/geo_pyspark_installed/points.csv",
            Offset=4,
            splitter=FileDataSplitter.WKT,
            carryInputData=True
        )

        point_rdd.analyze()
        cnt = point_rdd.countWithoutDuplicates()
        assert cnt == 12872, f"Point RDD should have 12872 but found {cnt}"

    def test_creating_polygon_rdd(self):
        polygon_rdd = PolygonRDD(
            sparkContext=spark._sc,
            InputLocation="/home/pkocinski001/Desktop/projects/geo_pyspark_installed/counties_tsv.csv",
            startingOffset=2,
            endingOffset=3,
            splitter=FileDataSplitter.WKT,
            carryInputData=True
        )

        polygon_rdd.analyze()

        cnt = polygon_rdd.countWithoutDuplicates()

        assert cnt == 407, f"Polygon RDD should have 407 but found {cnt}"