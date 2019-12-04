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