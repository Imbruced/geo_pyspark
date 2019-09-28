import attr
from pyspark.sql.types import UserDefinedType, StructType, StructField, DataType, StringType, ArrayType, ByteType
from pyspark.sql import types as t
from shapely.geometry.base import BaseGeometry

from geo_pyspark.sql.geometry import GeometryFactory


class GeometryType(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return ArrayType(ByteType(), containsNull=False)

    def fromInternal(self, obj):
        return self.deserialize(obj)

    def toInternal(self, obj):
        return self.serialize(obj)

    def serialize(self, obj):
        return [0, 1, 0, 0, 0, 0, 0, 0, 53, 64, 0, 0, 0, 0, 0, 0, 74, 64, 1, 3, 1, -127]

    def deserialize(self, datum):
        geom = GeometryFactory.geometry_from_bytes(datum)

        return geom

    @classmethod
    def module(cls):
        pass

    def needConversion(self):
        return True

    @classmethod
    def scalaUDT(cls):
        return "org.apache.spark.sql.geosparksql.UDT.GeometryUDT"
