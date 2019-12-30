from typing import Optional

import attr
from pyspark import SparkContext

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geo_pyspark.core.enums.file_data_splitter import FileSplitterJvm
from geo_pyspark.utils.types import path, crs


@attr.s
class LineStringRDD(SpatialRDD):
    sparkContext = attr.ib(type=Optional[SparkContext], default=None)
    InputLocation = attr.ib(type=Optional[path], default=None)
    splitter = attr.ib(type=Optional[str], default=None)
    carryInputData = attr.ib(type=Optional[bool], default=None)
    partitions = attr.ib(type=Optional[int], default=None)
    newLevel = attr.ib(type=Optional[str], default=None)
    sourceEpsgCRSCode = attr.ib(type=crs, default=None)
    targetEpsgCode = attr.ib(type=Optional[crs], default=None)
    spatialRDD = attr.ib(type=Optional['SpatialRDD'], default=None)
    startingOffset = attr.ib(default=None, type=Optional[int])
    endingOffset = attr.ib(default=None, type=Optional[int])

    def __attrs_post_init__(self):
        if self.spatialRDD is not None:
            self._srdd = self.spatialRDD._srdd
        if self.splitter is not None and self.sparkContext is not None:
            self._jvm_splitter = FileSplitterJvm(self.sparkContext._jvm, self.splitter).jvm_instance

        super().__attrs_post_init__()
        if self.sparkContext is not None:
            LineStringRDD = SpatialRDDFactory(self.sparkContext).create_linestring_rdd()

            self._srdd = LineStringRDD(
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
