import attr
from pyspark.sql import SparkSession
from pyspark.sql.types import UserDefinedType


@attr.s
class GeoSparkRegistrator:

    @classmethod
    def registerAll(cls, spark: SparkSession) -> bool:
        spark._jvm.\
            org.\
            imbruced.\
            geo_pyspark.\
            GeoSparkWrapper.\
            registerAll()
        return True
