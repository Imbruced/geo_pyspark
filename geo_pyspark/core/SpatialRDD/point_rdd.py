from typing import Optional

import attr

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory


@attr.s
class PointRDD(SpatialRDD):
    Offset = attr.ib(default=None, type=Optional[int])

    def __attrs_post_init__(self):
        super().__attrs_post_init__()

    def srdd_from_attributes(self):
        PointRDD = SpatialRDDFactory(self.sparkContext).create_point_rdd()

        point_rdd = PointRDD(
            self._jsc,
            self.InputLocation,
            self.Offset,
            self.splitter,
            self.carryInputData
        )

        return point_rdd
