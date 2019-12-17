import os

import pytest
from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.register import upload_jars, GeoSparkRegistrator

upload_jars()

spark = SparkSession.builder.\
    master("local[*]").\
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)

resource_folder = "resources"
point_rdd_input_location = os.path.join(resource_folder, "arealm-small.csv")
sc = spark.sparkContext
point_rdd_offset = 1
point_rdd_splitter = FileDataSplitter.CSV


class TestRDDSerialization:

    def test_point_rdd(self):
        point_rdd = PointRDD(
            sparkContext=sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )

        collected_points = point_rdd.getRawSpatialRDD().collect()

        points_coordinates = [
            [-88.331492, 32.324142], [-88.175933, 32.360763],
            [-88.388954, 32.357073], [-88.221102, 32.35078]
        ]

        assert [[point.x, point.y] for point in collected_points[:4]] == points_coordinates[:4]

    def test_polygon_rdd(self):
        pass

    def test_circle_rdd(self):
        pass

    def test_linestring_rdd(self):
        pass

    def test_rectangle_rdd(self):
        pass


