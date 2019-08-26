from pyspark.sql.types import DataType, UserDefinedType, StructType, StructField


from geo_pyspark.gis.geometry import GeometryFactory


class GeometryType(DataType):
    pass


class Geometry(UserDefinedType):

    @classmethod
    def sqlType(cls):
        return StructType(
            [StructField("geometry", GeometryType(), False)]
        )

    def fromInternal(self, obj):
        return self.deserialize(obj)

    def serialize(self, obj):
        pass

    def deserialize(self, datum):
        gm_type = GeometryFactory.from_number(datum[1])

        return gm_type.from_bytes(datum)

    @classmethod
    def module(cls):
        pass
