from pyspark import RDD

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.meta import MultipleMeta
from geo_pyspark.utils.rdd_pickling import GeoSparkPickler


class RangeQuery(metaclass=MultipleMeta):

    @classmethod
    @require([GeoSparkLib.RangeQuery])
    def SpatialRangeQuery(self, spatialRDD: SpatialRDD, rangeQueryWindow: Envelope, considerBoundaryIntersection: bool, usingIndex: bool):
        """

        :param spatialRDD:
        :param rangeQueryWindow:
        :param considerBoundaryIntersection:
        :param usingIndex:
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        jvm_envelope = rangeQueryWindow.create_jvm_instance(jvm)

        srdd = jvm.\
            RangeQuery.SpatialRangeQuery(
            spatialRDD._srdd,
            jvm_envelope,
            considerBoundaryIntersection,
            usingIndex
        )

        serlialized = jvm.GeoSerializerData.serializeToPython(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())
