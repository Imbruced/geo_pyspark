from py4j.java_gateway import java_import
from pyspark import RDD, SparkContext
from pyspark.java_gateway import local_connect_and_auth
from pyspark.rdd import _load_from_socket
from pyspark.sql import SparkSession, DataFrame
from pyspark.traceback_utils import SCCallSiteSync

from geo_pyspark.core.rdd import PointRDD

spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()


point_rdd = PointRDD(
    spark._sc,
    "/home/pkocinski001/Desktop/projects/geo_pyspark_installed/counties_tsv.csv",
    2,
    "WKT",
    True
)

RDD
SparkContext

# with SCCallSiteSync(self.context) as css:
#     sock_info = self.ctx._jvm.PythonRDD.collectAndServe(self._jrdd.rdd())
# return list(_load_from_socket(sock_info, self._jrdd_deserializer))

local_connect_and_auth

print(point_rdd.analyze())

print(point_rdd.countWithoutDuplicates())