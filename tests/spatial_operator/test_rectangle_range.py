import os

import pytest
from pyspark import StorageLevel
from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD import RectangleRDD
from geo_pyspark.core.enums import IndexType, GridType, FileDataSplitter
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.core.spatialOperator import RangeQuery
from geo_pyspark.register import upload_jars, GeoSparkRegistrator
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


class TestRectangleRange:
    query_envelope = Envelope(-90.01, -80.01, 30.01, 40.01)
    loop_times = 5

    def test_spatial_range_query(self):
        spatial_rdd = RectangleRDD(sc, inputLocation, offset, splitter, True, StorageLevel.MEMORY_ONLY)

        for i in range(self.loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                spatial_rdd, self.query_envelope, False, False).count()
            assert result_size == 193

        assert RangeQuery.SpatialRangeQuery(
            spatial_rdd, self.query_envelope, False, False).take(10)[1].getUserData() is not None

    def test_spatial_range_query_using_index(self):
        spatial_rdd = RectangleRDD(
            sc, inputLocation, offset, splitter, True, StorageLevel.MEMORY_ONLY)

        spatial_rdd.buildIndex(IndexType.RTREE, False)
        for i in range(self.loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                spatial_rdd, self.query_envelope, False, True).count()
            assert result_size == 193

        assert RangeQuery.SpatialRangeQuery(spatial_rdd, self.query_envelope, False, True).take(10)[1].getUserData()\
               is not None
