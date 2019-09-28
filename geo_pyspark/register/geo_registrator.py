import attr
from pyspark.sql import SparkSession
from shapely.geometry import Point, MultiPoint, Polygon, MultiPolygon, LineString, MultiLineString

from geo_pyspark.sql.types import GeometryType

Point.__UDT__ = GeometryType()
MultiPoint.__UDT__ = GeometryType()
Polygon.__UDT__ = GeometryType()
MultiPolygon.__UDT__ = GeometryType()
LineString.__UDT__ = GeometryType()
MultiLineString.__UDT__ = GeometryType()


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
