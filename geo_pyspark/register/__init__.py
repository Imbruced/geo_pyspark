import os
from os import path

import findspark

from geo_pyspark.register.geo_registrator import GeoSparkRegistrator


def find_spark_version() -> str:
    from pyspark.version import __version__
    major_version = __version__.split(".")[:-1]
    return "_".join(major_version)


abs_path = path.abspath(path.dirname(__file__))
module_path = os.path.join(*os.path.split(abs_path)[:-1])
spark_version = find_spark_version()
jars_path = os.path.join(module_path, "jars", spark_version)

findspark.add_jars(os.path.join(jars_path, "*"))
findspark.init(os.environ["SPARK_HOME"])

__all__ = ["GeoSparkRegistrator"]