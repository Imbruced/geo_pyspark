import attr
from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD.abstract import AbstractSpatialRDD
from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.core.jvm.geometry.primitive import JvmCoordinate, JvmPoint


@attr.s
class KNNQuery:

    @classmethod
    def SpatialKnnQuery(self, spatialRDD: AbstractSpatialRDD, originalQueryPoint: Point, k: int,  useIndex: bool):
        coordinate = JvmCoordinate(spatialRDD._jvm, 1.0, 1.0).create_jvm_instance()
        point = JvmPoint(spatialRDD._jvm, coordinate)

        res = spatialRDD.sparkContext._jvm.KNNQuery.SpatialKnnQuery(
            spatialRDD._srdd,
            point.create_jvm_instance(),
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
