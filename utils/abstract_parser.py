from abc import ABC

import attr
from shapely.geometry.base import BaseGeometry


@attr.s
class GeometryParser(ABC):

    @property
    def name(self):
        raise NotImplementedError

    @classmethod
    def serialize(cls):
        raise NotImplementedError("Parser has to implement serialize method")

    @classmethod
    def deserialize(cls, bytes: bytearray) -> BaseGeometry:
        raise NotImplementedError("Parser has to implement deserialize method")

    @classmethod
    def remove_negatives(cls, bytes):
        return [cls.remove_negative(bt) for bt in bytes]

    @classmethod
    def remove_negative(cls, byte):
        bt_pos = byte if byte >= 0 else byte + 128
        return bt_pos