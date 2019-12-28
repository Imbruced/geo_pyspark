from typing import Optional

import attr

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory


@attr.s
class PointRDD(SpatialRDD):
    Offset = attr.ib(default=None, type=Optional[int])

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        if self.InputLocation is not None and self.splitter is not None and self.sparkContext is not None:
            Spatial = SpatialRDDFactory(self.sparkContext).create_point_rdd()
            self._srdd = Spatial(
                self._jsc,
                self.InputLocation,
                self.Offset,
                self._jvm_splitter,
                self.carryInputData
            )
        else:
            self._srdd = None
