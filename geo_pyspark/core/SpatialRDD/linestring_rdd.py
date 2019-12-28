from typing import Optional

import attr

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory


@attr.s
class LineStringRDD(SpatialRDD):
    startingOffset = attr.ib(default=None, type=Optional[int])
    endingOffset = attr.ib(default=None, type=Optional[int])

    def srdd_from_attributes(self):
        LineStringRDD = SpatialRDDFactory(self.sparkContext).create_linestring_rdd()

        linestring_rdd = LineStringRDD(
            self._jsc,
            self.InputLocation,
            self.startingOffset,
            self.endingOffset,
            self.splitter,
            self.carryInputData
        )

        return linestring_rdd

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self._srdd = self.srdd_from_attributes()

    def MinimumBoundingRectangle(self):
        from geo_pyspark.core.SpatialRDD import RectangleRDD
        rectangle_rdd = RectangleRDD(
            spatialRDD=self,
            sparkContext=self.sparkContext
        )
        return rectangle_rdd
