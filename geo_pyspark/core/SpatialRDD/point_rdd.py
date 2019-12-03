import attr

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD


@attr.s
class PointRDD(SpatialRDD):

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self._srdd = self.__create_srdd()

    def __create_srdd(self):
        PointRDD = self._factory.create_point_rdd()

        point_rdd = PointRDD(
            self._jsc,
            self.InputLocation,
            self.Offset,
            self.splitter,
            self.carryInputData
        )

        return point_rdd
