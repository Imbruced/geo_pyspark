import struct

import attr


@attr.s
class BinaryParser:

    @classmethod
    def read_double(cls, bytes: bytearray):
        return struct.unpack("d", bytes)

    @classmethod
    def read_int(cls, bytes: bytearray):
        return struct.unpack("i", bytes)

    @classmethod
    def read_byte(cls, bytes: bytearray):
        return struct.unpack("b", bytes)

    @classmethod
    def remove_negatives(cls, bytes):
        return [cls.remove_negative(bt) for bt in bytes]

    @classmethod
    def remove_negative(cls, byte):
        bt_pos = byte if byte >= 0 else byte + 256
        return bt_pos
