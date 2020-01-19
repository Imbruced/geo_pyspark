import os

from tests.tools import tests_path

undefined_type_shape_location = os.path.join(tests_path, "resources/shapefiles/undefined")
polygon_shape_location = os.path.join(tests_path, "resources/shapefiles/polygon")
#
from geo_pyspark.core.formatMapper.shapefileParser import ShapefileReader
from tests.test_base import TestBase


class TestShapeFileReader(TestBase):

    def test_shape_file_end_with_undefined_type(self):
        shape_rdd = ShapefileReader.readToGeometryRDD(
            sc=self.sc, inputPath=undefined_type_shape_location
        )
        assert shape_rdd.fieldNames == ['LGA_CODE16', 'LGA_NAME16', 'STE_CODE16', 'STE_NAME16', 'AREASQKM16']
        assert shape_rdd.getRawSpatialRDD().count() == 545

    def test_read_geometry_rdd(self):
        shape_rdd = ShapefileReader.readToGeometryRDD(
            self.sc, polygon_shape_location
        )
        assert shape_rdd.fieldNames == []
        assert shape_rdd.rawSpatialRDD.collect().__len__() == 10000

    def test_read_to_polygon_rdd(self):
        # TODO add this test and implement loading to polygon rdd
        pass

    def test_read_to_linestring_rdd(self):
        # TODO add this test and implement loading to linestring rdd
        pass

    def test_read_to_point_rdd(self):
        # TODO add this test and implement loading to point rdd
        pass

    def test_read_to_point_rdd_multipoint(self):
        # TODO add this test and implement loading to point rdd with multipoint handling
        pass
