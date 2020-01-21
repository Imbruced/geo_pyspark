from re import findall
from typing import Optional
import logging

from pyspark.sql import SparkSession

from geo_pyspark.utils.decorators import classproperty

FORMAT = '%(asctime) %(message)s'
logging.basicConfig(format=FORMAT)


def compare_versions(version_a: str, version_b: str) -> bool:
    version_numbers = version_a.split("."), version_b.split(".")
    for ver_a, ver_b in zip(*version_numbers):
        if ver_a < version_b:
            return False
    if any(version_numbers):
        return False
    return True


def since(version: str):
    def wrapper(function):
        def applier(*args, **kwargs):
            result = function(*args, **kwargs)
            geo_spark_version = GeoSparkMeta.version
            if compare_versions(geo_spark_version, version):
                logging.warning(f"This function is not available for {geo_spark_version}, "
                                f"please use version higher than {version}")
            return result
        return applier
    return wrapper


def depreciated(version: str, substitute: str):
    def wrapper(function):
        def applier(*args, **kwargs):
            result = function(*args, **kwargs)
            geo_spark_version = GeoSparkMeta.version
            if geo_spark_version >= version:
                logging.warning("Function is depreciated")
                if substitute:
                    logging.warning(f"Please use {substitute} instead")
            return result
        return applier
    return wrapper


class SparkJars:

    @staticmethod
    def get_used_jars():
        spark = SparkSession._instantiatedSession
        if spark is not None:
            spark_conf = spark.conf
        else:
            raise TypeError("SparkSession is not initiated")
        java_spark_conf = spark_conf._jconf
        try:
            used_jar_files = java_spark_conf.get("spark.jars")
        except Exception as e:
            raise AttributeError("Can not find GeoSpark jars")
        return used_jar_files

    @property
    def jars(self):
        if not hasattr(self, "__spark_jars"):
            setattr(self, "__spark_jars", self.get_used_jars())
        return getattr(self, "__spark_jars")


class GeoSparkMeta:

    @classmethod
    def get_version(cls, spark_jars: str) -> Optional[str]:
        geospark_version = findall(r"geospark-(\d{1}\.\d{1}\.\d{1})\.jar", spark_jars)
        try:
            version = geospark_version[0]
        except IndexError:
            version = None
        return version

    @classproperty
    def version(cls):
        spark_jars = SparkJars.get_used_jars()
        if not hasattr(cls, "__version"):
            setattr(cls, "__version", cls.get_version(spark_jars))
        return getattr(cls, "__version")


if __name__ == "__main__":
    from geo_pyspark.register import upload_jars
    upload_jars()
    spark = SparkSession. \
        builder. \
        master("local[*]"). \
        getOrCreate()
    assert GeoSparkMeta.version == "1.2.0"