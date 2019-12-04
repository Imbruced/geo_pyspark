import attr

from geo_pyspark.core.SpatialRDD import spatial_rdd
from geo_pyspark.core.utils import get_geospark_package_location


class JoinQuery:

    @classmethod
    def SpatialJoinQuery(
            cls,
            spatialRDD: spatial_rdd,
            queryRDD: spatial_rdd,
            useIndex: bool,
            considerBoundaryIntersection: bool
    ):
        jvm_join = cls._jvm_spatial_join(spatialRDD._jvm)
        return jvm_join(
            spatialRDD._srdd,
            queryRDD._srdd,
            useIndex,
            considerBoundaryIntersection
        )

    @classmethod
    def _jvm_spatial_join(self, jvm):
        geospark = get_geospark_package_location(jvm)
        return geospark.spatialOperator.JoinQuery.SpatialJoinQuery