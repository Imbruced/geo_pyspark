import attr
from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.core.jvm.geometry.primitive import JvmCoordinate, JvmPoint


@attr.s
class KNNQuery:

    @classmethod
    def SpatialKnnQuery(self, spatialRDD: SpatialRDD, originalQueryPoint: Point, k: int,  useIndex: bool):
        """

        :param spatialRDD: spatialRDD
        :param originalQueryPoint: shapely.geometry.Point
        :param k: int
        :param useIndex: bool
        :return: pyspark.RDD
        """

        coordinate = JvmCoordinate(
            spatialRDD._jvm,
            originalQueryPoint.x,
            originalQueryPoint.y
        )

        point = JvmPoint(spatialRDD._jvm, coordinate.jvm_instance)
        jvm_point = point.jvm_instance

        jvm_knn = JvmKNNQuery(
            spatialRDD.sparkContext._jvm,
            spatialRDD._srdd,
            jvm_point,
            k,
            useIndex)

        res = jvm_knn.SpatialKnnQuery()

        return res


@attr.s
class JvmKNNQuery(JvmObject):
    spatialRDD = attr.ib()
    originalQueryPoint = attr.ib()
    k = attr.ib()
    useIndex = attr.ib()

    def SpatialKnnQuery(self):
        knn_neighbours = self.jvm.KNNQuery.SpatialKnnQuery(
                self.spatialRDD._srdd,
                self.originalQueryPoint,
                self.k,
                self.useIndex
        )
        return knn_neighbours
