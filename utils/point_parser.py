from abc import ABC

import attr

from utils.abstract_parser import Parser
from utils.binary_parser import BinaryParser


@attr.s
class PointParser(Parser):

    @classmethod
    def deserialize(cls, bytes: bytearray) -> 'Point':
        from gis.geometry import Point
        no_neg = cls.remove_negatives(bytes)
        x = BinaryParser.read_double(bytearray(no_neg[2: 10]))
        y = BinaryParser.read_double(bytearray(no_neg[10: 18]))
        return Point(x=x, y=y)
