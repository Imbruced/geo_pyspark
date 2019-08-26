from abc import ABC

import attr


@attr.s
class Parser(ABC):

    @classmethod
    def deserialize(cls, bytes: bytearray):
        raise NotImplementedError()

    @classmethod
    def remove_negatives(cls, bytes):
        return [cls.remove_negative(bt) for bt in bytes]

    @classmethod
    def remove_negative(cls, byte):
        bt_pos = byte if byte >= 0 else byte + 128
        return bt_pos