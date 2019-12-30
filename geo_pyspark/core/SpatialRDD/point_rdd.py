from typing import Optional

import attr
from pyspark import SparkContext

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geo_pyspark.core.enums.file_data_splitter import FileSplitterJvm
from geo_pyspark.utils.types import path, crs


@attr.s
class PointRDD(SpatialRDD):
    sparkContext = attr.ib(type=Optional[SparkContext], default=None)
    InputLocation = attr.ib(type=Optional[path], default=None)
    splitter = attr.ib(type=Optional[str], default=None)
    carryInputData = attr.ib(type=Optional[bool], default=None)
    partitions = attr.ib(type=Optional[int], default=None)
    newLevel = attr.ib(type=Optional[str], default=None)
    sourceEpsgCRSCode = attr.ib(type=crs, default=None)
    targetEpsgCode = attr.ib(type=Optional[crs], default=None)
    spatialRDD = attr.ib(type=Optional['SpatialRDD'], default=None)
    Offset = attr.ib(default=None, type=Optional[int])

    def __attrs_post_init__(self):
        if self.spatialRDD is not None:
            self._srdd = self.spatialRDD._srdd
        if self.splitter is not None and self.sparkContext is not None:
            self._jvm_splitter = FileSplitterJvm(self.sparkContext._jvm, self.splitter).jvm_instance
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
