import os

import pytest
from py4j.java_gateway import get_field
from pyspark import RDD
from pyspark.serializers import AutoBatchedSerializer, CloudPickleSerializer, FramedSerializer, PickleSerializer
from pyspark.sql import SparkSession

# from geo_pyspark.core.formatMapper.shapefileParser.ShapefileReader import ShapefileReader
from geo_pyspark.core.SpatialRDD import PolygonRDD
from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.register import upload_jars
from geo_pyspark.sql.geometry import GeometryFactory


import logging
import os
import pytest

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark import SparkContext
from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD import PointRDD, PolygonRDD, CircleRDD
from geo_pyspark.core.enums import GridType, FileDataSplitter, IndexType
from geo_pyspark.core.enums.join_build_side import JoinBuildSide
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.core.spatialOperator import range_query, RangeQuery, KNNQuery, JoinQuery
from geo_pyspark.core.spatialOperator.join_params import JoinParams
from geo_pyspark.register import upload_jars
import os
from geo_pyspark.register import GeoSparkRegistrator

upload_jars()


spark = SparkSession.builder.\
    master("local[*]").\
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)

resource_folder = "/home/pkocinski001/Desktop/projects/geo_pyspark/tests/resources"

point_rdd_input_location = os.path.join(resource_folder, "arealm.csv")

point_rdd_splitter = FileDataSplitter.CSV

point_rdd_index_type = IndexType.RTREE
point_rdd_num_partitions = 5
point_rdd_offset = 1

polygon_rdd_input_location = os.path.join(resource_folder, "primaryroads-polygon.csv")
polygon_rdd_splitter = FileDataSplitter.CSV
polygon_rdd_index_type = IndexType.RTREE
polygon_rdd_num_partitions = 5
polygon_rdd_start_offset = 0
polygon_rdd_end_offset = 9


knn_query_point = Point(-84.01, 34.01)

range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)

join_query_partitionin_type = GridType.QUADTREE
each_query_loop_times = 1

sc = spark.sparkContext

object_rdd = PointRDD(
    sparkContext=sc,
    InputLocation=point_rdd_input_location,
    Offset=0,
    splitter=point_rdd_splitter,
    carryInputData=False
)

geo_json_input_location = "/home/pkocinski001/Desktop/projects/GeoSpark/sql/src/test/resources/testPolygon.json"
spatial_rdd = PolygonRDD(
    spark.sparkContext,
    geo_json_input_location,
    FileDataSplitter.GEOJSON,
    True
)

object_rdd.analyze()
object_rdd.spatialPartitioning(join_query_partitionin_type)
spatial_rdd.spatialPartitioning(object_rdd.getPartitioner)

result_size = JoinQuery.SpatialJoinQuery(
    object_rdd,
    spatial_rdd,
    False,
    False)

from pyspark import RDD
from geo_pyspark.utils.serde import GeoSparkPickler

python_rdd = RDD(spark._jvm.GeoSerializerData.serializeToPython(spatial_rdd._srdd.getRawSpatialRDD()),
    spark._sc, GeoSparkPickler())

spatial_rdd.analyze()
spatial_rdd.spatialPartitioning(GridType.EQUALGRID)
field = get_field(spatial_rdd._srdd, "grids")

data = python_rdd.collect()[0]
print(data)
