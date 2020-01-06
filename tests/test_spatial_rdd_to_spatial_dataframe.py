import os

import pytest
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.register import GeoSparkRegistrator, upload_jars
from geo_pyspark.sql.types import GeometryType
from geo_pyspark.utils.prep import assign_all
from tests.utils import tests_path
from shapely.geometry import Point

upload_jars()


spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()

GeoSparkRegistrator.\
    registerAll(spark)

sc = spark.sparkContext

point_input_path = os.path.join(tests_path, "resources/arealm-small.csv")

crs_test_point = os.path.join(tests_path, "resources/crs-test-point.csv")

offset = 1
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"
numPartitions = 11


class TestSpatialRDDToDataFrame:

    def test_list_to_rdd_and_df(self):
        assign_all()
        point_data = [
            [Point(21, 52.0), "1", 1],
            [Point(22, 52.0), "2", 2],
            [Point(23.0, 52), "3", 3],
            [Point(23, 54), "4", 4],
            [Point(24.0, 56.0), "5", 5]
        ]
        schema = StructType(
            [
                StructField("geom", GeometryType(), False),
                StructField("id_1", StringType(), False),
                StructField("id_2", IntegerType(), False),
            ]
        )

        rdd_data = spark.sparkContext.parallelize(point_data)
        df = spark.createDataFrame(rdd_data)
        df.show()
        df.printSchema()


    def test_polygon_rdd(self):
        pass

    def test_rectangle_rdd(self):
        pass

    def test_point_rdd(self):
        spatial_rdd = PointRDD(
            sparkContext=sc,
            InputLocation=crs_test_point,
            Offset=0,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        raw_spatial_rdd = spatial_rdd.rawSpatialRDD.map(
            lambda x: [x.geom, *x.getUserData().split("\t")]
        )

        spark.createDataFrame(raw_spatial_rdd).show()

        schema = StructType(
            [
                StructField("geom", GeometryType()),
                StructField("name", StringType())
            ]
        )

        spatial_rdd_with_schema = spark.createDataFrame(
            raw_spatial_rdd, schema
        )

        spatial_rdd_with_schema.show()

        assert spatial_rdd_with_schema.take(1)[0][0].wkt == "POINT (32.324142 -88.331492)"

    def test_linestring_rdd(self):
        pass

    def test_circle_rdd(self):
        pass
