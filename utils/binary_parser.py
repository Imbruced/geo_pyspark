import struct
from typing import List, Union

import attr

DOUBLE_SIZE = 8
INT_SIZE = 4


@attr.s
class BinaryParser:

    @classmethod
    def read_double(cls, bytes: Union[bytearray, List[int]]):
        if type(bytes) == list:
            bytes = bytearray(bytes)
        return struct.unpack("d", bytes)

    @classmethod
    def read_int(cls, bytes: Union[bytearray, List[int]]):
        if type(bytes) == list:
            bytes = bytearray(bytes)
        return struct.unpack("i", bytes)

    @classmethod
    def read_byte(cls, bytes: Union[bytearray, List[int]]):
        if type(bytes) == list:
            bytes = bytearray(bytes)
        return struct.unpack("b", bytes)

    @classmethod
    def remove_negatives(cls, bytes):
        return [cls.remove_negative(bt) for bt in bytes]

    @classmethod
    def remove_negative(cls, byte):
        bt_pos = byte if byte >= 0 else byte + 256
        return bt_pos

    @staticmethod
    def _convert_to_binary_array(bytes):
        if type(bytes) == list:
            bytes = bytearray(bytes)
        return bytes
