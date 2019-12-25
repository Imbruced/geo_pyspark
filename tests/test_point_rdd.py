import pytest
from pyspark import StorageLevel
from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import IndexType
from geo_pyspark.register import GeoSparkRegistrator, upload_jars

upload_jars()

spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()

GeoSparkRegistrator.\
    registerAll(spark)

sc = spark.sparkContext

inputLocation = "arealm-small.csv"
queryWindowSet = "zcta510-small.csv"
offset = 1
splitter = "csv"
gridType = "rtree"
indexType = "rtree"
numPartitions = 11
distance = 0.01
queryPolygonSet = "primaryroads-polygon.csv"
inputCount = 3000
inputBoundary = -173.120769, -84.965961, 30.244859, 71.355134
rectangleMatchCount = 103
rectangleMatchWithOriginalDuplicatesCount = 103
polygonMatchCount = 472
polygonMatchWithOriginalDuplicatesCount = 562


class TestPointRDD:

    def test_constructor(self):
        spatial_rdd = PointRDD(
            sparkContext=sc,
            InputLocation=inputLocation,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )
        assert inputCount == spatial_rdd.approximateTotalCount
        assert inputBoundary == spatial_rdd.boundaryEnvelope
        assert spatial_rdd.rawSpatialRDD.take(9).get(0).getUserData().equals("testattribute0\ttestattribute1\ttestattribute2")
        assert spatial_rdd.rawSpatialRDD.take(9).get(2).getUserData().equals("testattribute0\ttestattribute1\ttestattribute2")
        assert spatial_rdd.rawSpatialRDD.take(9).get(4).getUserData().equals("testattribute0\ttestattribute1\ttestattribute2")
        assert spatial_rdd.rawSpatialRDD.take(9).get(8).getUserData().equals("testattribute0\ttestattribute1\ttestattribute2")

    def test_empty_constructor(self):
        pass

    def test_equal_partitioning(self):
        pass

    def test_hilbert_curve_spatial_partitioning(self):
        pass

    def test_r_tree_spatial_partitioning(self):
        pass

    def test_voronoi_spatial_partitioning(self):
        pass

    def test_build_index_without_set_grid(self):
        pass

    def test_build_r_tree_index(self):
        pass

    def test_build_quadtree_index(self):
        spatial_rdd = PointRDD(
            sparkContext=sc,
            InputLocation=inputLocation,
            Offset=offset,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY

        )

        spatial_rdd.spatialPartitioning(gridType)
        spatial_rdd.buildIndex(IndexType.QUADTREE, True)

        spatial_rdd.indexedRDD()
