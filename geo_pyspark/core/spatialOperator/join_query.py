import attr

from geo_pyspark.core.jvm.abstract import JvmObject


class JoinQuery:

    @classmethod
    def SpatialJoinQuery(
            cls,
            spatialRDD,
            queryRDD,
            useIndex: bool,
            considerBoundaryIntersection: bool
    ):
        JvmSpatialJoinQuery(
            spatialRDD._jvm,
            spatialRDD,
            queryRDD,
            useIndex,
            considerBoundaryIntersection
        )


class JvmSpatialJoinQuery(JvmObject):
    spatialRDD = attr.ib()
    queryRDD = attr.ib()
    useIndex = attr.ib()
    considerBoundaryIntersection = attr.ib()

    @property
    def jvm_reference(self):
        return "org.datasyslab.geospark.spatialOperator.JoinQuery.SpatialJoinQuery"

    def create_jvm_instance(self):
        return self.get_reference()

    def SpatialJoinQuery(self):
        spatial_join = self.create_jvm_instance()
        return spatial_join(
            self.spatialRDD._srdd,
            self.queryRDD._srdd,
            self.useIndex,
            self.considerBoundaryIntersection
        )
