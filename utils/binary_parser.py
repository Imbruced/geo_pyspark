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
