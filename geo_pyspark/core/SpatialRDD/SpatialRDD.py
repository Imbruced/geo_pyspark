from abc import ABC
from typing import Optional

import attr
from pyspark import SparkContext

from geo_pyspark.core.SpatialRDD.SpatialRDDFactory import SpatialRDDFactory
from geo_pyspark.core.enums.GridType import GridTypeJvm
from geo_pyspark.core.utils import FileSplitterJvm
from geo_pyspark.utils.types import path, crs


@attr.s
class SpatialRDD(ABC):

    sparkContext = attr.ib(type=SparkContext)
    InputLocation = attr.ib(type=Optional[path], default=None)
    splitter = attr.ib(type=Optional[str], default=None)
    carryInputData = attr.ib(type=Optional[bool], default=None)
    Offset = attr.ib(type=Optional[int], default=None)
    partitions = attr.ib(type=Optional[int], default=None)
    newLevel = attr.ib(type=Optional[str], default=None)
    sourceEpsgCRSCode = attr.ib(type=crs, default=None)
    targetEpsgCode = attr.ib(type=Optional[crs], default=None)
    startingOffset = attr.ib(type=Optional[int], default=None)
    endingOffset = attr.ib(type=Optional[int], default=None)

    def __attrs_post_init__(self):
        self._jsc = self.sparkContext._jsc
        self.__file_spliter_jvm = FileSplitterJvm(self.sparkContext)
        self.splitter = self.__file_spliter_jvm.get_splitter(self.splitter)
        self._factory = SpatialRDDFactory(self.sparkContext)
        self._jvm = self.sparkContext._jvm

    @classmethod
    def from_srdd(cls, sc: SparkContext, jsrdd):
        instance = cls(sparkContext=sc, InputLocation=None, splitter=None, carryInputData=False)
        instance._srdd = jsrdd

        return instance

    def __create_srdd(self):
        raise NotImplementedError()

    def analyze(self) -> bool:
        """

        :return: bool,
        """
        return self._srdd.analyze()

    def CRSTransform(self, sourceEpsgCRSCode: crs, targetEpsgCRSCode: crs) -> bool:
        """
        Function transforms coordinates from one crs to another one
        :param sourceEpsgCRSCode: crs,  Cooridnate Reference System to transform from
        :param targetEpsgCRSCode: crs, Coordinate Reference System to transform to
        :return: bool, True if transforming was correct
        """
        return self._srdd.CRSTransform(sourceEpsgCRSCode, targetEpsgCRSCode)

    def MinimumBoundingRectangle(self):
        return self._srdd.MinimumBoundingRectangle()

    def approximateTotalCount(self):
        return self._srdd.approximateTotalCount()

    def boundary(self):
        return self._srdd.boundary()

    def boundaryEnvelope(self):
        raise NotImplementedError()

    def buildIndex(self):
        raise NotImplementedError()

    def countWithoutDuplicates(self):
        return self._srdd.countWithoutDuplicates()

    def countWithoutDuplicatesSPRDD(self):
        raise NotImplementedError()

    def ensuring(self):
        raise NotImplementedError()

    def eq(self):
        raise NotImplementedError()

    def equals(self):
        raise NotImplementedError()

    def fieldNames(self):
        raise NotImplementedError()

    def formatted(self):
        raise NotImplementedError()

    def getCRStransformation(self):
        raise NotImplementedError()

    def getClass(self):
        raise NotImplementedError()

    @property
    def getPartitioner(self) -> str:
        return self._srdd.getPartitioner()

    def getRawSpatialRDD(self):
        return self._srdd.getRawSpatialRDD()

    def getSampleNumber(self):
        raise NotImplementedError()

    def getSourceEpsgCode(self):
        raise NotImplementedError()

    def getTargetEpsggCode(self):
        raise NotImplementedError()

    def grids(self):
        raise NotImplementedError()

    def indexedRDD(self):
        raise NotImplementedError()

    def indexedRawRDD(self):
        raise NotImplementedError()

    def partitionTree(self):
        raise NotImplementedError()

    def rawSpatialRDD(self):
        raise NotImplementedError()

    def saveAsGeoJSON(self, path: str):
        return self._srdd.saveAsGeoJSON(path)

    def setRawSpatialRDD(self):
        raise NotImplementedError()

    def setSampleNumber(self):
        raise NotImplementedError()

    def spatialPartitionedRDD(self):
        raise NotImplementedError()

    def spatialPartitioning(self, partitioning: str):
        if type(partitioning) == str:
            grid = GridTypeJvm(self._jvm)
            current_grid_type = grid.get_grid_type(partitioning)
        else:
            current_grid_type = partitioning
        return self._srdd.spatialPartitioning(
            current_grid_type
        )
