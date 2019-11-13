import os

import pytest
from pyspark.sql import SparkSession

from geo_pyspark.core.formatMapper.shapefileParser.ShapefileReader import ShapefileReader
from geo_pyspark.data import data_path
from geo_pyspark.register import upload_jars

upload_jars()


spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()



shape_files_path = os.path.join(data_path, "shapefiles")
polygon_path = os.path.join(shape_files_path, "polygon")
point_path = os.path.join(shape_files_path, "point")
line_string_path = os.path.join(shape_files_path, "polyline")


class TestSpatialFilesReader:

    def test_load_shapefile(self):
        shape = ShapefileReader.readToGeometryRDD(
            spark.sparkContext,
            polygon_path
        )




