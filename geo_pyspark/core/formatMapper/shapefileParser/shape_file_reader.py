import attr
from pyspark import SparkContext

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.core.utils import require
from geo_pyspark.exceptions import InvalidParametersException
from geo_pyspark.register.java_libs import GeoSparkLib


@attr.s
class ShapefileReader:

    @classmethod
    @require([GeoSparkLib.ShapeFileReader])
    def readToGeometryRDD(cls, sc: SparkContext, inputPath: str) -> SpatialRDD:
        jvm_shape_file_reader = JvmShapefileReader(jvm=sc._jvm, sparkContext=sc, inputPath=inputPath)
        return jvm_shape_file_reader.readToGeometryRDD()


@attr.s
class JvmShapefileReader(JvmObject):
    sparkContext = attr.ib(default=None, type=SparkContext)
    inputPath = attr.ib(default=None, type=str)

    def readToGeometryRDD(self) -> SpatialRDD:
        if self.sparkContext is not None and self.inputPath is not None:
            jvm = self.sparkContext._jvm

            srdd = jvm.ShapefileReader.readToGeometryRDD(
                self.sparkContext._jsc,
                self.inputPath
            )

            spatial_rdd = SpatialRDD(sparkContext=self.sparkContext)
            spatial_rdd._srdd = srdd
            return spatial_rdd

        else:
            raise InvalidParametersException("Parsed parameters are incorrect")