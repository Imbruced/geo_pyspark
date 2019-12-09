import logging

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from geo_pyspark.core.SpatialRDD import PolygonRDD
from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.core.formatMapper.shapefileParser import ShapefileReader
from geo_pyspark.register import GeoSparkRegistrator
from geo_pyspark.register import upload_jars
from geo_pyspark.utils.adapter import Adapter
from tests.data import geojson_input_location, shape_file_with_missing_trailing_input_location

upload_jars()

spark = SparkSession.\
    builder.\
    master("local[*]").\
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestAdapter:

    def test_read_csv_point_into_spatial_rdd(self):
        pass

    def test_csv_point_at_different_column_id_into_spatial_rdd(self):
        pass

    def test_read_csv_point_at_a_different_column_col_name_into_a_spatial_rdd(self):
        pass

    def test_read_csv_point_into_a_spatial_rdd_by_passing_coordinates(self):
        pass

    def test_read_csv_point_into_s_spatial_rdd_with_unique_id_by_passing_coordinates(self):
        pass

    def test_read_mixed_wkt_geometries_into_spatial_rdd(self):
        pass

    def test_read_mixed_wkt_geometries_into_spatial_rdd_with_unique_id(self):
        pass

    def test_read_shapefile_to_dataframe(self):
        pass

    def test_read_shapefile_with_missing_to_dataframe(self):
        spatial_rdd = ShapefileReader.\
            readToGeometryRDD(spark.sparkContext, shape_file_with_missing_trailing_input_location)

        spatial_rdd.analyze()
        logging.info(spatial_rdd.fieldNames)

        df = Adapter.toDf(spatial_rdd, spark)
        df.show()

    def test_geojson_to_dataframe(self):
        spatial_rdd = PolygonRDD(
            spark.sparkContext, geojson_input_location, FileDataSplitter.GEOJSON, True
        )

        spatial_rdd.analyze()

        df = Adapter.toDf(spatial_rdd, spark).\
            withColumn("geometry", expr("ST_GeomFromWKT(geometry)"))
        df.show()
        assert (df.columns[1] == "STATEFP")