import attr

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD


@attr.s
class RectangleRDD(SpatialRDD):

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self._srdd = self.__create_srdd()

    def __create_srdd(self):
        pass
