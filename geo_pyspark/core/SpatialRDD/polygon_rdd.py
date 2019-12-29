from typing import Optional

import attr

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory


@attr.s
class PolygonRDD(SpatialRDD):
    startingOffset = attr.ib(default=None, type=Optional[int])
    endingOffset = attr.ib(default=None, type=Optional[int])

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        if all([self.sparkContext]):
            PolygonRDD = SpatialRDDFactory(self.sparkContext).create_polygon_rdd()

            self._srdd = PolygonRDD(
                self._jsc,
                self.InputLocation,
                self.startingOffset,
                self.endingOffset,
                self._jvm_splitter,
                self.carryInputData
            )
        else:
            self._srdd = None

    def MinimumBoundingRectangle(self):
        from geo_pyspark.core.SpatialRDD import RectangleRDD
        rectangle_rdd = RectangleRDD(
            spatialRDD=self,
            sparkContext=self.sparkContext
        )
        return rectangle_rdd
