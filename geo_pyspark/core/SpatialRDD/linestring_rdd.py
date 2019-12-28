from typing import Optional

import attr

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory


@attr.s
class LineStringRDD(SpatialRDD):
    startingOffset = attr.ib(default=None, type=Optional[int])
    endingOffset = attr.ib(default=None, type=Optional[int])

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        if self.sparkContext is not None:
            LineStringRDD = SpatialRDDFactory(self.sparkContext).create_linestring_rdd()

            self._srdd = LineStringRDD(
                self._jsc,
                self.InputLocation,
                self.startingOffset,
                self.endingOffset,
                self.splitter,
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
