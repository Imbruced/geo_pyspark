from typing import Optional

import attr
from pyspark import SparkContext, RDD

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.formatMapper.geo_reader import GeoDataReader
from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.core.utils import require
from geo_pyspark.exceptions import InvalidParametersException
from geo_pyspark.register.java_libs import GeoSparkLib


class GeoJsonReader(GeoDataReader):

    @classmethod
    @require([GeoSparkLib.GeoJsonReader])
    def readToGeometryRDD(cls,
                          sparkContext: Optional[SparkContext] = None,
                          inputPath: Optional[str] = None,
                          allowInvalidGeometries: Optional[bool] = True,
                          skipSyntaticallyInvalidGeometries: Optional[bool] = False,
                          rawTextRDD: Optional[RDD] = None) -> SpatialRDD:
        """
        :param sparkContext:
        :param inputPath:
        :param allowInvalidGeometries:
        :param skipSyntaticallyInvalidGeometries:
        :param rawTextRDD:
        :return:
        """

        jvm_geo_json_reader = JvmGeoJsonReader(
            jvm=sparkContext._jvm,
            sparkContext=sparkContext,
            inputPath=inputPath,
            allowInvalidGeometries=allowInvalidGeometries,
            skipSyntacticallyInvalidGeometries=skipSyntaticallyInvalidGeometries,
            rawTextRDD=rawTextRDD
        )
        return jvm_geo_json_reader.readToGeometryRDD()


@attr.s
class JvmGeoJsonReader(JvmObject):
    sparkContext = attr.ib(type=SparkContext, default=None)
    inputPath = attr.ib(type=str, default=None)
    allowInvalidGeometries = attr.ib(type=bool, default=None)
    skipSyntacticallyInvalidGeometries = attr.ib(type=bool, default=None)
    rawTextRDD = attr.ib(type=RDD, default=None)

    def readToGeometryRDD(self) -> SpatialRDD:
        if self.sparkContext is not None and self.inputPath is not None:
            jvm = self.sparkContext._jvm
            srdd = jvm.GeoJsonReader.readToGeometryRDD(
                self.sparkContext._jsc,
                self.inputPath,
                self.allowInvalidGeometries,
                self.skipSyntacticallyInvalidGeometries
            )

            spatial_rdd = SpatialRDD(sparkContext=self.sparkContext)
            spatial_rdd._srdd = srdd
            return spatial_rdd

        elif self.sparkContext is None and self.inputPath is None and self.rawTextRDD is not None:
            jvm = self.rawTextRDD.ctx._jvm
            srdd = jvm.GeoJsonReader.readToGeometryRDD(
                self.rawTextRDD._jrdd
            )

            spatial_rdd = SpatialRDD(sparkContext=self.rawTextRDD.ctx)
            spatial_rdd._srdd = srdd
            return spatial_rdd

        else:
            raise InvalidParametersException("Parsed parameters are incorrect")