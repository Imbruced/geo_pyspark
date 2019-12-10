import attr

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.core.spatialOperator.join_params import JoinParams


class JoinQuery:

    @classmethod
    def SpatialJoinQuery(cls, spatialRDD, queryRDD, useIndex: bool, considerBoundaryIntersection: bool):
        return JvmSpatialJoinQuery(
            spatialRDD._jvm,
            spatialRDD,
            queryRDD,
            useIndex,
            considerBoundaryIntersection
        ).SpatialJoinQuery()

    @classmethod
    def DistanceJoinQuery(cls, spatialRDD, queryRDD, useIndex: bool, considerBoundaryIntersection: bool):
        return JvmDistanceJoinQuery(
            spatialRDD._jvm,
            spatialRDD,
            queryRDD,
            useIndex,
            considerBoundaryIntersection
        ).DistanceJoinQuery()

    @classmethod
    def spatialJoin(cls, queryWindowRDD: SpatialRDD, objectRDD: SpatialRDD, joinParams: JoinParams):
        """
        TODO check if circle rdd works here also
        """

        return JvmSpatialJoin(queryWindowRDD._jvm, queryWindowRDD, objectRDD, joinParams).spatialJoin()

    @classmethod
    def DistanceJoinQueryFlat(cls, spatialRDD, queryRDD, useIndex: bool, considerBoundaryIntersection: bool):
        return JvmDistanceJoinQueryFlat(
            spatialRDD._jvm,
            spatialRDD,
            queryRDD,
            useIndex,
            considerBoundaryIntersection
        ).spatialJoin()


@attr.s
class JvmSpatialJoinQuery(JvmObject):
    spatialRDD = attr.ib()
    queryRDD = attr.ib()
    useIndex = attr.ib()
    considerBoundaryIntersection = attr.ib()

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
    spatialRDD = attr.ib()
    queryRDD = attr.ib()
    useIndex = attr.ib()
    considerBoundaryIntersection = attr.ib()

    def create_jvm_instance(self):
        return self.jvm.org.\
            datasyslab.\
            geospark.\
            spatialOperator.\
            JoinQuery.\
            DistanceJoinQuery

    def DistanceJoinQuery(self):
        spatial_join = self.create_jvm_instance()
        return spatial_join(
            self.spatialRDD._srdd,
            self.queryRDD._srdd,
            self.useIndex,
            self.considerBoundaryIntersection
        )


@attr.s
class JvmSpatialJoin(JvmObject):
    queryRDD = attr.ib()
    objectRDD = attr.ib()
    joinParams = attr.ib()

    def create_jvm_instance(self):
        return self.jvm.org.\
            datasyslab.\
            geospark.\
            spatialOperator.\
            JoinQuery. \
            spatialJoin

    def spatialJoin(self):
        spatial_join = self.create_jvm_instance()
        jvm_join_params = self.joinParams.jvm_instance(self.objectRDD._jvm)
        return spatial_join(
            self.queryRDD._srdd,
            self.objectRDD._srdd,
            jvm_join_params
        )


@attr.s
class JvmDistanceJoinQueryFlat(JvmObject):
    spatialRDD = attr.ib()
    queryRDD = attr.ib()
    useIndex = attr.ib()
    considerBoundaryIntersection = attr.ib()

    def create_jvm_instance(self):
        return self.jvm.org.\
            datasyslab.\
            geospark.\
            spatialOperator.\
            JoinQuery. \
            DistanceJoinQueryFlat

    def spatialJoin(self):
        spatial_join = self.create_jvm_instance()
        return spatial_join(
            self.spatialRDD._srdd,
            self.queryRDD._srdd,
            self.useIndex,
            self.considerBoundaryIntersection
        )