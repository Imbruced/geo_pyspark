import os

import pytest
from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD import PointRDD, PolygonRDD
from geo_pyspark.core.enums import FileDataSplitter, IndexType
from geo_pyspark.register import upload_jars, GeoSparkRegistrator

upload_jars()

spark = SparkSession.builder. \
    master("local[*]"). \
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)

resource_folder = "resources"
point_rdd_input_location = os.path.join(resource_folder, "arealm-small.csv")
polygon_rdd_input_location = os.path.join(resource_folder, "primaryroads-polygon.csv")
polygon_rdd_splitter = FileDataSplitter.CSV
polygon_rdd_index_type = IndexType.RTREE
polygon_rdd_num_partitions = 5
polygon_rdd_start_offset = 0
polygon_rdd_end_offset = 9

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
        polygon_rdd = PolygonRDD(
            sparkContext=sc,
            InputLocation=polygon_rdd_input_location,
            startingOffset=polygon_rdd_start_offset,
            endingOffset=polygon_rdd_end_offset,
            splitter=polygon_rdd_splitter,
            carryInputData=True
        )

        collected_polygon_rdd = polygon_rdd.getRawSpatialRDD().collect()

        input_wkt_polygons = [
            "POLYGON ((-74.020753 40.836454, -74.020753 40.843768, -74.018162 40.843768, -74.018162 40.836454, -74.020753 40.836454))",
            "POLYGON ((-74.018978 40.837712, -74.018978 40.852181, -74.014938 40.852181, -74.014938 40.837712, -74.018978 40.837712))",
            "POLYGON ((-74.021683 40.833253, -74.021683 40.834288, -74.021368 40.834288, -74.021368 40.833253, -74.021683 40.833253))"
        ]

        assert [polygon.wkt for polygon in collected_polygon_rdd][:3] == input_wkt_polygons

    def test_circle_rdd(self):
        pass

    def test_linestring_rdd(self):
        pass

    def test_rectangle_rdd(self):
        pass
