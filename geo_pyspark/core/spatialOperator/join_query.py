import attr

from geo_pyspark.core.jvm.abstract import JvmObject


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
