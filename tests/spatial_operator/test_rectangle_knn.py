import os

import pytest
from pyspark.sql import SparkSession
from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD import RectangleRDD
from geo_pyspark.core.enums import IndexType, FileDataSplitter
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.core.spatialOperator import KNNQuery
from geo_pyspark.register import upload_jars, GeoSparkRegistrator
from geo_pyspark.utils.spatial_rdd_parser import GeoData
from tests.utils import tests_path

upload_jars()

spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()

GeoSparkRegistrator.\
    registerAll(spark)

sc = spark.sparkContext

inputLocation = os.path.join(tests_path, "resources/zcta510-small.csv")
queryWindowSet = os.path.join(tests_path, "resources/zcta510-small.csv")
offset = 0
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"
numPartitions = 11
distance = 0.001
queryPolygonSet = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
inputCount = 3000
inputBoundary = Envelope(-171.090042, 145.830505, -14.373765, 49.00127)
matchCount = 17599
matchWithOriginalDuplicatesCount = 17738


def distance_sorting_functions(geo_data: GeoData, query_point: Point):
    return geo_data.geom.distance(query_point)


class TestRectangleKNN:
    query_envelope = Envelope(-90.01, -80.01, 30.01, 40.01)
    loop_times = 5
    query_point = Point(-84.01, 34.01)
    top_k = 100

    def test_spatial_knn_query(self):
        rectangle_rdd = RectangleRDD(sc, inputLocation, offset, splitter, True)

        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(rectangle_rdd, self.query_point, self.top_k, False)

            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_query_using_index(self):
        rectangle_rdd = RectangleRDD(sc, inputLocation, offset, splitter, True)
        rectangle_rdd.buildIndex(IndexType.RTREE, False)

        for i in range(self.loop_times):
            result = KNNQuery.SpatialKnnQuery(rectangle_rdd, self.query_point, self.top_k, False)

            assert result.__len__() > -1
            assert result[0].getUserData() is not None

    def test_spatial_knn_query_correctness(self):
        rectangle_rdd = RectangleRDD(sc, inputLocation, offset, splitter, True)

        result_no_index = KNNQuery.SpatialKnnQuery(rectangle_rdd, self.query_point, self.top_k, False)
        rectangle_rdd.buildIndex(IndexType.RTREE, False)

        result_with_index = KNNQuery.SpatialKnnQuery(rectangle_rdd, self.query_point, self.top_k, True)

        sorted_result_no_index = sorted(result_no_index, key=lambda geo_data: distance_sorting_functions(
            geo_data, self.query_point))

        sorted_result_with_index = sorted(result_with_index, key=lambda geo_data: distance_sorting_functions(
            geo_data, self.query_point))

        difference = 0
        for x in range(self.top_k):
            difference += sorted_result_no_index[x].geom.distance(sorted_result_with_index[x].geom)

        assert difference == 0
