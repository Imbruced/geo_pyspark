from unittest import TestCase

from shapely.geometry import Point as ShapelyPoint
from shapely.geometry import MultiPoint as ShapelyMultiPoint
from shapely.geometry import LineString as ShapelyLineString
from shapely.geometry import MultiLineString as ShapelyMultiLineString

from geo_pyspark.sql.base_geom import Point, MultiPoint, LineString, MultiLineString
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
        linestring = LineString([[21.00, 52.00], [22.00, 52.00], [24.00, 54.00]])
        self.assertEqual(isinstance(linestring, ShapelyLineString), True)
        self.assertEqual(type(linestring.__UDT__), GeometryType)

    def test_multilinestring(self):
        linestring = MultiLineString([[[21.00, 52.00], [22.00, 52.00], [24.00, 54.00]]])
        self.assertEqual(isinstance(linestring, ShapelyMultiLineString), True)
        self.assertEqual(type(linestring.__UDT__), GeometryType)

    def test_polygon(self):
        pass

    def test_multipolygon(self):
        pass

    def test_convert_to_geopandas(self):
        pass

    def test_convert_to_geospark(self):
        pass
