import pytest
import os

import pytest
from py4j.java_gateway import get_field
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD import RectangleRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.enums import IndexType, FileDataSplitter, GridType
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.core.spatialOperator import KNNQuery, JoinQuery
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


class TestRectangleJoin:

    def partition_rdds(self, query_rdd: SpatialRDD, spatial_rdd: SpatialRDD, index: GridType):
        spatial_rdd.spatialPartitioning(index)
        query_rdd.spatialPartitioning(spatial_rdd.getPartitioner)

    def create_rectangle_rdd(self):
        rdd = RectangleRDD(sc, inputLocation, splitter, True, numPartitions)
        rdd.analyze()
        return RectangleRDD(rdd.rawJvmSpatialRDD, StorageLevel.MEMORY_ONLY)

    def test_nested_loop(self):
        """TODO add sanity check"""
        query_rdd = self.create_rectangle_rdd()
        spatial_rdd = self.create_rectangle_rdd()

        for index in GridType:
            self.partition_rdds(query_rdd, spatial_rdd, index)

            result = JoinQuery.SpatialJoinQuery(
                spatial_rdd, query_rdd, False, True).collect()

            count = 0
            for el in result:
                count += el[1].__len__()
            assert matchCount == count

        # sanityCheckJoinResults(result);
        # assertEquals(expectedMatchCount, countJoinResults(result));
