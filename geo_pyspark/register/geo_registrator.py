import attr
from pyspark.sql import SparkSession

from geo_pyspark.utils.prep import assign_all

assign_all()


@attr.s
class GeoSparkRegistrator:

    @classmethod
    def registerAll(cls, spark: SparkSession) -> bool:
        spark.sql("SELECT 1 as geom").count()
        cls.register(spark)
        return True

    @classmethod
    def register(cls, spark: SparkSession):
        spark._jvm. \
            org. \
            imbruced. \
            geo_pyspark. \
            GeoSparkWrapper. \
            registerAll()
