from unittest import TestCase

from geo_pyspark.gis.geometry import GeometryFactory, Undefined, Point
from geo_pyspark.utils.binary_parser import BinaryParser


class TestNonSparkGeometry(TestCase):

    def test_geometry_factory(self):
        # tp = GeometryFactory.from_number(0)
        # self.assertEqual(type(tp), Undefined)

        tp = GeometryFactory.from_number(1)
        self.assertEqual(tp, Point)

    def test_binary_parser_double(self):
        bt = bytearray([0, 0, 0, 0, 0, 0, 24, -64 + 128])

        value = BinaryParser.read_double(bt)[0]

        self.assertEqual(value, 6.0)

    def test_binary_parser_int(self):
        pass

    def test_binary_parser_byte(self):
        pass

