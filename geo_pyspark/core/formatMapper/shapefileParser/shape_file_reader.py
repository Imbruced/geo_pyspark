import attr
from pyspark import SparkContext

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.formatMapper.geo_reader import GeoDataReader
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.meta import MultipleMeta


@attr.s
class ShapefileReader(GeoDataReader, metaclass=MultipleMeta):

    @classmethod
    @require([GeoSparkLib.ShapeFileReader])
    def validate_imports(cls):
        return True

    @classmethod
    def readToGeometryRDD(cls, sc: SparkContext, inputPath: str) -> SpatialRDD:
        ShapefileReader.validate_imports()
        jvm = sc._jvm
        jsc = sc._jsc
        srdd = jvm.ShapefileReader.readToGeometryRDD(
            jsc,
            inputPath
        )
        spatial_rdd = SpatialRDD(sparkContext=sc)

        spatial_rdd.set_srdd(srdd)
        return spatial_rdd
