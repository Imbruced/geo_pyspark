from pyspark import RDD

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.spatialOperator.join_params import JoinParams
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.rdd_pickling import GeoSparkPickler


class JoinQuery:

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def SpatialJoinQuery(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool):
        """

        :param spatialRDD:
        :param queryRDD:
        :param useIndex:
        :param considerBoundaryIntersection:
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        srdd = jvm.JoinQuery.SpatialJoinQuery(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )
        serlialized = jvm.GeoSerializerData.serializeToPythonHashSet(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def DistanceJoinQuery(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool):
        """

        :param spatialRDD:
        :param queryRDD:
        :param useIndex:
        :param considerBoundaryIntersection:
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc
        srdd = jvm.JoinQuery.DistanceJoinQuery(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )
        serlialized = jvm.GeoSerializerData.serializeToPythonHashSet(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def spatialJoin(cls, queryWindowRDD: SpatialRDD, objectRDD: SpatialRDD, joinParams: JoinParams):
        """

        :param queryWindowRDD:
        :param objectRDD:
        :param joinParams:
        :return:
        """

        jvm = queryWindowRDD._jvm
        sc = queryWindowRDD._sc

        jvm_join_params = joinParams.jvm_instance(jvm)

        srdd = jvm.JoinQuery.spatialJoin(queryWindowRDD._srdd, objectRDD._srdd, jvm_join_params)

        serlialized = jvm.GeoSerializerData.serializeToPython(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def DistanceJoinQueryFlat(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool):
        """

        :param spatialRDD:
        :param queryRDD:
        :param useIndex:
        :param considerBoundaryIntersection:
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        spatial_join = jvm.JoinQuery.DistanceJoinQueryFlat
        srdd = spatial_join(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )

        serlialized = jvm.GeoSerializerData.serializeToPython(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def SpatialJoinQueryFlat(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool,
                              considerBoundaryIntersection: bool):
        """

        :param spatialRDD:
        :param queryRDD:
        :param useIndex:
        :param considerBoundaryIntersection:
        :return:
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        spatial_join = jvm.JoinQuery.SpatialJoinQueryFlat
        srdd = spatial_join(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )

        serlialized = jvm.GeoSerializerData.serializeToPython(srdd)

        return RDD(serlialized, sc, GeoSparkPickler())
