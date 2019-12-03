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

    def srdd_from_attributes(self):
        PolygonRDD = SpatialRDDFactory(self.sparkContext).create_polygon_rdd()

        polygon_rdd = PolygonRDD(
            self._jsc,
            self.InputLocation,
            self.startingOffset,
            self.endingOffset,
            self.splitter,
            self.carryInputData
        )

        return polygon_rdd
