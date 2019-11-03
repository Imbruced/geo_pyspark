import pytest
from pyspark.sql import SparkSession

from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.core.rdd import PointRDD
from geo_pyspark.register import upload_jars

upload_jars()


spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()


class TestPointRDD:

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
        assert cnt == 12872, f"Point Rdd should have {12872} but found {cnt}"

