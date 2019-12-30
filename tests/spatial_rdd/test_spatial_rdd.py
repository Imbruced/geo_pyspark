import os

import pytest
from pyspark import StorageLevel, RDD
from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import IndexType, GridType
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.core.utils import ImportedJvmLib
from geo_pyspark.register import GeoSparkRegistrator, upload_jars
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.serde import GeoSparkPickler
from tests.utils import tests_path
from py4j.java_gateway import get_field

upload_jars()


spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()

GeoSparkRegistrator.\
    registerAll(spark)

sc = spark.sparkContext

input_file_location = os.path.join(tests_path, "resources/arealm-small.csv")
crs_test_point = os.path.join(tests_path, "resources/crs-test-point.csv")

offset = 1
splitter = "csv"
gridType = "rtree"
indexType = "rtree"
numPartitions = 11


def create_spatial_rdd():
    spatial_rdd = PointRDD(
        sparkContext=sc,
        InputLocation=input_file_location,
        Offset=offset,
        splitter=splitter,
        carryInputData=True,
        partitions=numPartitions,
        newLevel=StorageLevel.MEMORY_ONLY
    )
    return spatial_rdd


class TestSpatialRDD:

    def test_analyze(self):
        spatial_rdd = create_spatial_rdd()
        assert spatial_rdd.analyze()

    def test_crs_transform(self):
        spatial_rdd = PointRDD(
            sparkContext=sc,
            InputLocation=crs_test_point,
            Offset=0,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.CRSTransform("epsg:4326", "epsg:3857")

        assert spatial_rdd.rawSpatialRDD.collect()[0].geom.wkt == "POINT (-9833016.710450118 3805934.914254189)"

    def test_minimum_bounding_rectangle(self):
        pass

    def test_approximate_total_count(self):
        spatial_rdd = create_spatial_rdd()
        assert spatial_rdd.approximateTotalCount == 100

    def test_boundary(self):
        spatial_rdd = create_spatial_rdd()
        envelope = spatial_rdd.boundary()

        assert envelope == Envelope(minx=-173.120769, maxx=-84.965961, miny=30.244859, maxy=71.355134)

    def test_boundary_envelope(self):
        spatial_rdd = create_spatial_rdd()
        spatial_rdd.analyze()
        assert Envelope(
            minx=-173.120769, maxx=-84.965961, miny=30.244859, maxy=71.355134) == spatial_rdd.boundaryEnvelope

    def test_build_index(self):
        pass

    def test_count_without_duplicates(self):
        pass

    def test_field_names(self):
        pass

    def test_get_crs_transformation(self):
        pass

    def test_get_partitioner(self):
        pass

    def test_get_raw_spatial_rdd(self):
        pass

    def test_get_sample_number(self):
        pass

    def test_get_source_epsg_code(self):
        spatial_rdd = PointRDD(
            sparkContext=sc,
            InputLocation=crs_test_point,
            Offset=0,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        assert spatial_rdd.getSourceEpsgCode() == ""

        spatial_rdd.CRSTransform("epsg:4326", "epsg:3857")

        assert spatial_rdd.getSourceEpsgCode() == "epsg:4326"

    def test_get_target_epsg_code(self):
        spatial_rdd = PointRDD(
            sparkContext=sc,
            InputLocation=crs_test_point,
            Offset=0,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        assert spatial_rdd.getTargetEpsgCode() == ""

        spatial_rdd.CRSTransform("epsg:4326", "epsg:3857")

        assert spatial_rdd.getTargetEpsgCode() == "epsg:3857"

    def test_grids(self):
        pass

    def test_indexed_rdd(self):
        spatial_rdd = create_spatial_rdd()
        srdd = spatial_rdd._srdd.indexedRDD
        rdd = RDD(srdd, sc, GeoSparkPickler())
        print(rdd.collect())

    def test_indexed_raw_rdd(self):
        pass

    def test_partition_tree(self):
        pass

    def test_raw_spatial_rdd(self):
        pass

    def test_save_as_geojson(self):
        pass

    def test_set_raw_spatial_rdd(self):
        pass

    def test_set_sample_number(self):
        pass

    def test_spatial_partitioned_rdd(self):
        pass

    def test_spatial_partitioning(self):
        pass