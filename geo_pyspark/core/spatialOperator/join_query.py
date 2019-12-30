import attr
from pyspark import RDD

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.core.spatialOperator.join_params import JoinParams
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.serde import GeoSparkPickler


class JoinQuery:

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def SpatialJoinQuery(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool):
        return JvmSpatialJoinQuery(
            spatialRDD._jvm,
            spatialRDD,
            queryRDD,
            useIndex,
            considerBoundaryIntersection
        ).SpatialJoinQuery()

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def DistanceJoinQuery(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool):
        return JvmDistanceJoinQuery(
            spatialRDD._jvm,
            spatialRDD,
            queryRDD,
            useIndex,
            considerBoundaryIntersection
        ).DistanceJoinQuery()

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def spatialJoin(cls, queryWindowRDD: SpatialRDD, objectRDD: SpatialRDD, joinParams: JoinParams):
        """
        TODO check if circle rdd works here also
        """

        return JvmSpatialJoin(queryWindowRDD._jvm, queryWindowRDD, objectRDD, joinParams).spatialJoin()

    @classmethod
    @require([GeoSparkLib.JoinQuery])
    def DistanceJoinQueryFlat(cls, spatialRDD: SpatialRDD, queryRDD: SpatialRDD, useIndex: bool, considerBoundaryIntersection: bool):
        return JvmDistanceJoinQueryFlat(
            spatialRDD._jvm,
            spatialRDD,
            queryRDD,
            useIndex,
            considerBoundaryIntersection
        ).spatialJoin()


@attr.s
class JvmSpatialJoinQuery(JvmObject):
    spatialRDD = attr.ib(type=SpatialRDD)
    queryRDD = attr.ib(type=SpatialRDD)
    useIndex = attr.ib(type=bool)
    considerBoundaryIntersection = attr.ib(type=bool)

    def create_jvm_instance(self):
        return self.jvm.org.\
            datasyslab.\
            geospark.\
            spatialOperator.\
            JoinQuery.\
            SpatialJoinQuery

    def SpatialJoinQuery(self):
        spatial_join = self.create_jvm_instance()
        return spatial_join(
            self.spatialRDD._srdd,
            self.queryRDD._srdd,
            self.useIndex,
            self.considerBoundaryIntersection
        )


@attr.s
class JvmDistanceJoinQuery(JvmObject):
    spatialRDD = attr.ib(type=SpatialRDD)
    queryRDD = attr.ib(type=SpatialRDD)
    useIndex = attr.ib(type=bool)
    considerBoundaryIntersection = attr.ib(type=bool)

    def _create_jvm_instance(self):
        return self.jvm.JoinQuery.DistanceJoinQuery

    def DistanceJoinQuery(self):
        spatial_join = self.jvm_instance
        return spatial_join(
            self.spatialRDD._srdd,
            self.queryRDD._srdd,
            self.useIndex,
            self.considerBoundaryIntersection
        )


@attr.s
class JvmSpatialJoin(JvmObject):
    queryRDD = attr.ib(type=SpatialRDD)
    objectRDD = attr.ib(type=SpatialRDD)
    joinParams = attr.ib(type=JoinParams)

    def _create_jvm_instance(self):
        return self.jvm.JoinQuery.spatialJoin

    def spatialJoin(self) -> RDD:
        spatial_join = self.jvm_instance
        jvm_join_params = self.joinParams.jvm_instance(self.objectRDD._jvm)
        srdd = spatial_join(self.queryRDD._srdd, self.objectRDD._srdd, jvm_join_params)
        return RDD(self.queryRDD.sparkContext, srdd, )


@attr.s
class JvmDistanceJoinQueryFlat(JvmObject):
    spatialRDD = attr.ib(type=SpatialRDD)
    queryRDD = attr.ib(type=SpatialRDD)
    useIndex = attr.ib(type=bool)
    considerBoundaryIntersection = attr.ib(type=bool)

    def _create_jvm_instance(self):
        return self.jvm.JoinQuery.DistanceJoinQueryFlat

    def spatialJoin(self):
        spatial_join = self.jvm_instance
        srdd = spatial_join(
            self.spatialRDD._srdd,
            self.queryRDD._srdd,
            self.useIndex,
            self.considerBoundaryIntersection
        )

        return RDD(self.spatialRDD.sparkContext, srdd, GeoSparkPickler())
