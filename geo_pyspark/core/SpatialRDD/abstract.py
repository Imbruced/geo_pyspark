from abc import ABC
from typing import Optional

import attr
from pyspark import SparkContext

from geo_pyspark.core.enums.grid_type import GridTypeJvm
from geo_pyspark.core.enums.index_type import IndexTypeJvm
from geo_pyspark.core.utils import FileSplitterJvm
from geo_pyspark.utils.types import crs, path


@attr.s
class AbstractSpatialRDD(ABC):

    sparkContext = attr.ib(type=Optional[SparkContext], default=None)
    InputLocation = attr.ib(type=Optional[path], default=None)
    splitter = attr.ib(type=Optional[str], default=None)
    carryInputData = attr.ib(type=Optional[bool], default=None)
    partitions = attr.ib(type=Optional[int], default=None)
    newLevel = attr.ib(type=Optional[str], default=None)
    sourceEpsgCRSCode = attr.ib(type=crs, default=None)
    targetEpsgCode = attr.ib(type=Optional[crs], default=None)

    def __attrs_post_init__(self):
        self._jsc = self.sparkContext._jsc
        self.__file_spliter_jvm = FileSplitterJvm(self.sparkContext)
        self.splitter = self.__file_spliter_jvm.get_splitter(self.splitter)
        self._jvm = self.sparkContext._jvm
        self._srdd = self.srdd_from_attributes()

    def srdd_from_attributes(self):
        raise NotImplementedError("SpatialRDD instance has to implement srdd_from_attributes")

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
        raise self._srdd.boundaryEnvelope()

    def buildIndex(self, indexType: str, buildIndexOnSpatialPartitionedRDD: bool):
        return self._srdd.buildIndex(
            IndexTypeJvm(self._jvm).get_index_type(indexType),
            buildIndexOnSpatialPartitionedRDD
        )

    def countWithoutDuplicates(self):
        return self._srdd.countWithoutDuplicates()

    def countWithoutDuplicatesSPRDD(self):
        raise self._countWithoutDuplicatesSPRDD()

    def fieldNames(self):
        raise self._fieldNames()

    def getCRStransformation(self):
        raise self.getCRSTransformation()

    @property
    def getPartitioner(self) -> str:
        return self._srdd.getPartitioner()

    def getRawSpatialRDD(self):
        return self._srdd.getRawSpatialRDD()

    def getSampleNumber(self):
        return self._srdd.getSampleNumber()

    def getSourceEpsgCode(self):
        return self._srdd.getSourceEpsgCode()

    def getTargetEpsggCode(self):
        return self._srdd.getTargetEpsgCode()

    def grids(self):
        return self._srdd.grids()

    def indexedRDD(self):
        return self._srdd.indexedRDD()

    def indexedRawRDD(self):
        return self._srdd.indexedRawRDD()

    def partitionTree(self):
        raise self._srdd.partitionTree()

    def rawSpatialRDD(self):
        return self._srdd.rawSpatialRDD()

    def saveAsGeoJSON(self, path: str):
        return self._srdd.saveAsGeoJSON(path)

    def setRawSpatialRDD(self):
        raise self._setRawSpatialRDD()

    def setSampleNumber(self):
        raise self._srdd.setSampleNumber()

    def spatialPartitionedRDD(self):
        raise self._spatialPartitionedRDD()

    def spatialPartitioning(self, partitioning: str):
        if type(partitioning) == str:
            grid = GridTypeJvm(self._jvm)
            current_grid_type = grid.get_grid_type(partitioning)
        else:
            current_grid_type = partitioning
        return self._srdd.spatialPartitioning(
            current_grid_type
        )