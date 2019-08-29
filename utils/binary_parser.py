import struct
from typing import List, Union

import attr

DOUBLE_SIZE = 8
INT_SIZE = 4
BYTE_SIZE = 1

size_dict = {
    "d": DOUBLE_SIZE,
    "i": INT_SIZE,
    "b": BYTE_SIZE
}


@attr.s
class BinaryParser:

    def read_double(self):
        data = self.unpack("d", self.bytes)
        self.current_index = self.current_index + DOUBLE_SIZE
        return data

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
