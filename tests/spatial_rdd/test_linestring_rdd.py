import pytest
from pyspark import StorageLevel
from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD import LineStringRDD
from geo_pyspark.core.enums import IndexType, GridType
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.register import upload_jars, GeoSparkRegistrator

upload_jars()

spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()

GeoSparkRegistrator.\
    registerAll(spark)

sc = spark.sparkContext

inputLocation = "resources/primaryroads-linestring.csv"
queryWindowSet = "resources/zcta510-small.csv"
offset = 0
splitter = "csv"
gridType = "rtree"
indexType = "rtree"
numPartitions = 5
distance = 0.01
queryPolygonSet = "resources/primaryroads-polygon.csv"
inputCount = 3000
inputBoundary = Envelope(minx=-123.393766, maxx=-65.648659, miny=17.982169, maxy=49.002374)
matchCount = 535
matchWithOriginalDuplicatesCount = 875


class TestLineStringRDD:

    def test_constructor(self):
        spatial_rdd = LineStringRDD(
            sparkContext=sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()

        assert inputCount == spatial_rdd.approximateTotalCount
        assert inputBoundary == spatial_rdd.boundaryEnvelope

    def test_empty_constructor(self):
        spatial_rdd = LineStringRDD(
            sparkContext=sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(gridType)
        spatial_rdd.buildIndex(IndexType.RTREE, True)
        spatial_rdd_copy = LineStringRDD()
        spatial_rdd_copy.rawSpatialRDD = spatial_rdd
        spatial_rdd_copy.indexedRawRDD = spatial_rdd.indexedRawRDD
        spatial_rdd_copy.analyze()

    def test_hilbert_curve_spatial_partitioning(self):
        spatial_rdd = LineStringRDD(
            sparkContext=sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.HILBERT)
        for envelope in spatial_rdd.grids:
            print(envelope)

    def test_rtree_spatial_partitioning(self):
        spatial_rdd = LineStringRDD(
            sparkContext=sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.RTREE)
        for envelope in spatial_rdd.grids:
            print(envelope)

    def test_voronoi_spatial_partitioning(self):
        spatial_rdd = LineStringRDD(
            sparkContext=sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.VORONOI)
        for envelope in spatial_rdd.grids:
            print(envelope)

    def test_build_index_without_set_grid(self):
        spatial_rdd = LineStringRDD(
            sparkContext=sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.buildIndex(IndexType.RTREE, False)

    def test_build_rtree_index(self):
        pass
        # TODO add this test

    def test_build_quadtree_index(self):
        pass
        # TODO add this test

    def test_mbr(self):
        linestring_rdd = LineStringRDD(
            sparkContext=sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        rectangle_rdd = linestring_rdd.MinimumBoundingRectangle()
        result = rectangle_rdd.rawSpatialRDD.collect()

        for el in result:
            print(el)

        assert result.__len__() > -1
