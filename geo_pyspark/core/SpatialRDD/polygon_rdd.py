import attr

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD


@attr.s
class PolygonRDD(SpatialRDD):

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self._srdd = self.__create_srdd()

    def __create_srdd(self):
        PolygonRDD = self._factory.create_polygon_rdd()

        polygon_rdd = PolygonRDD(
            self._jsc,
            self.InputLocation,
            self.startingOffset,
            self.endingOffset,
            self.splitter,
            self.carryInputData
        )

        return polygon_rdd
