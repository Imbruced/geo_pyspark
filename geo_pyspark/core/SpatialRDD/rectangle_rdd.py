from typing import Optional

import attr
from pyspark import SparkContext

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geo_pyspark.core.enums.file_data_splitter import FileSplitterJvm
from geo_pyspark.utils.types import path, crs


@attr.s
class RectangleRDD(SpatialRDD):
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
        from geo_pyspark.core.SpatialRDD import PolygonRDD
        from geo_pyspark.core.SpatialRDD import LineStringRDD

        if self.spatialRDD is not None:
            self._srdd = self.spatialRDD._srdd
        if self.splitter is not None and self.sparkContext is not None:
            self._jvm_splitter = FileSplitterJvm(self.sparkContext._jvm, self.splitter).jvm_instance

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
