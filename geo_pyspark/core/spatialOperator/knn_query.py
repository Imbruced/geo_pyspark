import attr

from geo_pyspark.core.SpatialRDD.abstract import AbstractSpatialRDD


@attr.s
class KNNQuery:

    @classmethod
    def SpatialKnnQuery(self, spatialRDD: AbstractSpatialRDD, originalQueryPoint, k: int,  useIndex: bool):
        res = spatialRDD.sparkContext._jvm.\
            org.datasyslab.geospark.spatialOperator.KNNQuery.SpatialKnnQuery(
            spatialRDD._srdd,
            originalQueryPoint,
            k,
            useIndex
        )

        return res


@attr.s
class JvmKNNQuery(JvmObject):
    spatialRDD = attr.ib()
    originalQueryPoint = attr.ib()
    k = attr.ib()
    useIndex = attr.ib()

    def create_jvm_instance(self):
        return self.jvm.org.\
            datasyslab.\
            geospark.\
            spatialOperator.\
            KNNQuery.\
            SpatialKnnQuery

    def SpatialKnnQuery(self):
        spatial_knn_query = self.create_jvm_instance()
        return spatial_knn_query(
            self.spatialRDD._srdd,
            self.queryRDD._srdd,
            self.useIndex,
            self.considerBoundaryIntersection
        )
