from unittest import TestCase

from shapely.geometry import Point as ShapelyPoint

from geo_pyspark.sql.base_geom import Point
from geo_pyspark.sql.types import GeometryType


class TestGeometries(TestCase):

    def test_point(self):
        point = Point(21.0, 52.0)
        self.assertEqual(isinstance(point, ShapelyPoint), True)
        self.assertEqual(type(point.__UDT__), GeometryType)

    def test_multipoint(self):
        multipoint = MultiPoint([[21.00, 52.00], [22.00, 52.00]])
        self.assertEqual(isinstance(multipoint, ShapelyMultiPoint), True)
        self.assertEqual(type(multipoint.__UDT__), GeometryType)

    def test_linestring(self):
        pass

    def test_multilinestring(self):
        pass

    def test_polygon(self):
        pass

    def test_multipolygon(self):
        pass

    def test_convert_to_geopandas(self):
        pass

    def test_convert_to_geospark(self):
        pass
