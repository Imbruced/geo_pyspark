import inspect

from pyspark import SparkContext, RDD

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.formatMapper.geo_reader import GeoDataReader
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.meta import MultipleMeta


class GeoJsonReader(GeoDataReader, metaclass=MultipleMeta):

    @classmethod
    def validate_imports(cls) -> bool:
        return True

    @classmethod
    def readToGeometryRDD(cls, sc: SparkContext, inputPath: str):
        """

        :param sc:
        :param inputPath:
        :return:
        """
        GeoJsonReader.validate_imports()
        jvm = sc._jvm
        srdd = jvm.GeoJsonReader.readToGeometryRDD(
            sc._jsc, inputPath
        )

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToGeometryRDD(cls, sc: SparkContext, inputPath: str, allowInvalidGeometries: bool,
                          skipSyntacticallyInvalidGeometries: bool) -> SpatialRDD:
        """

        :param sc:
        :param inputPath:
        :param allowInvalidGeometries:
        :param skipSyntacticallyInvalidGeometries:
        :return:
        """
        GeoJsonReader.validate_imports()
        jvm = sc._jvm
        srdd = jvm.GeoJsonReader.readToGeometryRDD(
            sc._jsc, inputPath, allowInvalidGeometries, skipSyntacticallyInvalidGeometries
        )

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToGeometryRDD(cls, rawTextRDD: RDD):
        """

        :param rawTextRDD:
        :return:
        """
        GeoJsonReader.validate_imports()
        sc = rawTextRDD.ctx
        jvm = sc._jvm

        srdd = jvm.GeoJsonReader.readToGeometryRDD(
            rawTextRDD._jrdd
        )

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd

    @classmethod
    def readToGeometryRDD(cls, rawTextRDD: RDD, allowInvalidGeometries: bool, skipSyntacticallyInvalidGeometries: bool):
        """

        :param rawTextRDD:
        :param allowInvalidGeometries:
        :param skipSyntacticallyInvalidGeometries:
        :return:
        """
        GeoJsonReader.validate_imports()
        sc = rawTextRDD.ctx
        jvm = sc._jvm

        srdd = jvm.GeoJsonReader.readToGeometryRDD(
            rawTextRDD._jrdd, allowInvalidGeometries, skipSyntacticallyInvalidGeometries
        )

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd
