import os

import findspark

from geo_pyspark.register.geo_registrator import GeoSparkRegistrator


def find_spark_version() -> str:
    from pyspark.version import __version__
    major_version = __version__.split(".")[:-1]
    return ".".join(major_version)


spark_version = find_spark_version()

findspark.add_jars(f"geo_pyspark/jars/{spark_version}/*")
findspark.init(os.environ["SPARK_HOME"])

__all__ = ["GeoSparkRegistrator"]