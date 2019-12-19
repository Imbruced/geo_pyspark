import os

import pytest
from pyspark import RDD
from pyspark.serializers import AutoBatchedSerializer, CloudPickleSerializer, FramedSerializer, PickleSerializer
from pyspark.sql import SparkSession

# from geo_pyspark.core.formatMapper.shapefileParser.ShapefileReader import ShapefileReader
from geo_pyspark.core.SpatialRDD import PolygonRDD
from geo_pyspark.core.enums import FileDataSplitter
from .data import data_path
from geo_pyspark.register import upload_jars

upload_jars()


spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()


import base64
import re


def decode_base64(data, altchars=b'+/'):
    """Decode base64, padding being optional.

    :param data: Base64 data as an ASCII byte string
    :returns: The decoded byte string.

    """
    data = re.sub(rb'[^a-zA-Z0-9%s]+' % altchars, b'', data)  # normalize
    missing_padding = len(data) % 4
    if missing_padding:
        data += b'='* (4 - missing_padding)
    return base64.b64decode(data, altchars)


shape_files_path = os.path.join(data_path, "shapefiles")
polygon_path = os.path.join(shape_files_path, "polygon")
point_path = os.path.join(shape_files_path, "point")
line_string_path = os.path.join(shape_files_path, "polyline")
geojsonInputLocation = "/home/pawel/Desktop/projects/GeoSpark/sql/src/test/resources/testPolygon.json"
spatialRDD = PolygonRDD(spark.sparkContext, geojsonInputLocation, FileDataSplitter.GEOJSON, False)


class KocinakSerializer(PickleSerializer):
    def loads(self, obj, encoding="bytes"):
        return [el for el in obj]

    def dumps(self, obj):
        return super().dumps(obj)

serialized =spark._jvm.org.imbruced.geo_pyspark.GeoSerializer.serialize(spatialRDD.getRawSpatialRDD())
print("S")
rdd = RDD(serialized, spark._sc, AutoBatchedSerializer(KocinakSerializer()))
print("S")
rdd.collect()[0]
#
# class TestSpatialFilesReader:
#
#     def test_load_shapefile(self):
#         shape = ShapefileReader.readToGeometryRDD(
#             spark.sparkContext,
#             polygon_path
#         )




