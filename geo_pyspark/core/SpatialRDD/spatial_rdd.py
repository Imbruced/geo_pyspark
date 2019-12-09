from typing import Optional

import attr
from pyspark import SparkContext

from geo_pyspark.core.SpatialRDD.abstract import AbstractSpatialRDD
from geo_pyspark.core.utils import FileSplitterJvm
from geo_pyspark.utils.types import path, crs


@attr.s
class SpatialRDD(AbstractSpatialRDD):
    sparkContext = attr.ib(type=Optional[SparkContext], default=None)
    InputLocation = attr.ib(type=Optional[path], default=None)
    splitter = attr.ib(type=Optional[str], default=None)
    carryInputData = attr.ib(type=Optional[bool], default=None)
    partitions = attr.ib(type=Optional[int], default=None)
    newLevel = attr.ib(type=Optional[str], default=None)
    sourceEpsgCRSCode = attr.ib(type=crs, default=None)
    targetEpsgCode = attr.ib(type=Optional[crs], default=None)

    def __attrs_post_init__(self):
        if self.sparkContext is not None:
            self._jsc = self.sparkContext._jsc
            self._jvm = self.sparkContext._jvm

            if self.splitter is not None:
                self.__file_spliter_jvm = FileSplitterJvm(self.sparkContext)
                self.splitter = self.__file_spliter_jvm.get_splitter(self.splitter)
        else:
            self._jsc = None
            self._jvm = None

        super().__attrs_post_init__()

    def srdd_from_attributes(self):
        pass

    def set_srdd(self, srdd):
        self._srdd = srdd
