import os

from pyspark.sql import SparkSession
from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import GridType, FileDataSplitter, IndexType
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.core.spatialOperator import range_query, RangeQuery
from geo_pyspark.register import upload_jars

upload_jars()

spark = SparkSession.builder.\
    master("local[*]").\
    getOrCreate()

resource_folder = "geo_pyspark/data"

point_rdd_input_location = os.path.join(resource_folder, "arealm-small.csv")

point_rdd_splitter = FileDataSplitter.CSV

point_rdd_index_type = IndexType.RTREE
point_rdd_num_partitions = 5
point_rdd_offset = 1

polygon_rdd_input_location = os.path.join(resource_folder, "primaryroads-polygon.csv")
polygon_rdd_splitter = FileDataSplitter.CSV
polygon_rdd_index_type = IndexType.RTREE
polygon_rdd_num_partitions = 5
polygon_rdd_start_offset = 0
polygon_rdd_offset = 5


knn_query_point = Point(-84.01, 34.01)

range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)

join_query_partitionin_type = GridType.QUADTREE
each_query_loop_times = 1

sc = spark.sparkContext


class TestSpatialRDD:

    def test_empty_constructor_test(self):
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        object_rdd_copy = PointRDD()
        object_rdd_copy.rawSpatialRDD = object_rdd.rawSpatialRDD
        object_rdd_copy.analyze()

    def test_spatial_range_query(self):
        object_rdd = PointRDD(sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, False)
        for i in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                object_rdd, range_query_window, False, False
            ).count

    def test_range_query_using_index(self):
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        object_rdd.buildIndex(point_rdd_index_type, False)
        for i in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                object_rdd, range_query_window, False, True).count



