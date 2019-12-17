import attr
from pyspark.sql import SparkSession

from geo_pyspark.utils.prep import assign_all

assign_all()


@attr.s
class GeoSparkRegistrator:

    @classmethod
    def registerAll(cls, spark: SparkSession) -> bool:
        """
        This is the core of whole package, It uses py4j to run wrapper which takes existing SparkSession
        and register all User Defined Functions by GeoSpark developers, for this SparkSession.

        :param spark: pyspark.sql.SparkSession, spark session instance
        :return: bool, True if registration was correct.
        """
        spark.sql("SELECT 1 as geom").count()
        cls.register(spark)
        return True

    @classmethod
    def register(cls, spark: SparkSession):
        spark._jvm.registerAll()


class PackageImporter:

    @staticmethod
    def import_jvm_lib(jvm, jvm_libs: List[jvm_import]) -> bool:
        """
        Imports all the specified methods and functions in jvm
        :param jvm: Jvm gateway from py4j
        :param jvm_libs: List[str] of class, object function which to import
        :return:
        """
        for lib in jvm_libs:
            java_import(jvm, lib)

        return True
