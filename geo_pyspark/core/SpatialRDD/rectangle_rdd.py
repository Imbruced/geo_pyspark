from typing import Optional

import attr

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory


@attr.s
class RectangleRDD(SpatialRDD):
    Offset = attr.ib(default=None, type=Optional[int])

    def __attrs_post_init__(self):
        from geo_pyspark.core.SpatialRDD import PolygonRDD
        from geo_pyspark.core.SpatialRDD import LineStringRDD

        super().__attrs_post_init__()
        arguments = self.sparkContext, self.InputLocation, self.Offset, self.splitter, self.carryInputData, self.partitions
        arguments_is_none = [arg is not None for arg in arguments]

        if isinstance(self.spatialRDD, PolygonRDD) or isinstance(self.spatialRDD, LineStringRDD):
            self._srdd = self.spatialRDD._srdd.MinimumBoundingRectangle()
        elif all(arguments_is_none):
            rectangle_rdd = SpatialRDDFactory(self.sparkContext).\
                create_rectangle_rdd()
            j_rdd = rectangle_rdd(
                    self._jsc,
                    self.InputLocation,
                    self.Offset,
                    self._jvm_splitter,
                    self.carryInputData,
                    self.partitions
            )

            self._srdd = j_rdd
        else:
            self._srdd = None
