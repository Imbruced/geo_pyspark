import attr
from pyspark.sql import SparkSession
from geo_pyspark.sql.types import Geometry

@attr.s
class GeoSparkRegistrator:

    @classmethod
    def registerAll(cls, spark: SparkSession) -> bool:
        cls.register(spark)
        try:
            spark.sql("""SELECT st_geomfromtext('POINT(-6.0 52.0)') as geom""").count()
        except Exception:
            pass

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
