from abc import ABC

import attr
from pyspark import SparkContext
from pyspark.sql import SparkSession

from geo_pyspark.core.enums import FileSplitterJvm

crs = str
path = str


@attr.s
class SpatialRDDFactory(ABC):

    sparkContext = attr.ib(type=SparkContext)

    def __attrs_post_init__(self):
        self._jvm = self.sparkContext._jvm

    def create_point_rdd(self):
        return getattr(
            self._jvm,
            "org.datasyslab.geospark.spatialRDD.PointRDD"
        )

    def create_polygon_rdd(self):
        return getattr(
            self._jvm,
            "org.datasyslab.geospark.spatialRDD.PolygonRDD"
        )

    def create_linestring_rdd(self):
        return getattr(
            self._jvm,
            "org.datasyslab.geospark.spatialRDD.LineStringRDD"
        )

    def create_rectangle_rdd(self):
        return getattr(
            self._jvm,
            "org.datasyslab.geospark.spatialRDD.RectangleRDD"
        )

    def create_circle_rdd(self):
        return getattr(
            self._jvm,
            "org.datasyslab.geospark.spatialRDD.CircleRDD"
        )


@attr.s
class SpatialRDD(ABC):

    sparkContext = attr.ib(type=SparkContext)
    InputLocation = attr.ib(type=path)
    Offset = attr.ib(type=int)
    splitter = attr.ib(type=str)
    carryInputData = attr.ib(type=bool)
    partitions = attr.ib(type=int, default=None)
    newLevel = attr.ib(type=str, default=None)
    sourceEpsgCRSCode = attr.ib(type=crs, default=None)
    targetEpsgCode = attr.ib(type=crs, default=None)

    def __attrs_post_init__(self):
        self._jsc = self.sparkContext._jsc
        self.__file_spliter_jvm = FileSplitterJvm(self.sparkContext)
        self.splitter = self.__file_spliter_jvm.get_splitter(self.splitter)

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

    def getPartitioner(self):
        raise NotImplementedError()

    def getRawSpatialRDD(self):
        raise NotImplementedError()

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

    def saveAsGeoJSON(self):
        raise NotImplementedError()

    def setRawSpatialRDD(self):
        raise NotImplementedError()

    def setSampleNumber(self):
        raise NotImplementedError()

    def spatialPartitionedRDD(self):
        raise NotImplementedError()

    def spatialPartitioning(self):
        raise NotImplementedError()


@attr.s
class PointRDD(SpatialRDD):

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self._srdd = self.__create_srdd()

    def __create_srdd(self):
        PointRDD = SpatialRDDFactory(self.sparkContext).create_point_rdd()

        point_rdd = PointRDD(
            self._jsc,
            self.InputLocation,
            self.Offset,
            self.splitter,
            self.carryInputData
        )

        return point_rdd


@attr.s
class PolygonRDD(SpatialRDD):

    def __attrs_post_init__(self):
        self._srdd = self.__create_srdd()

    def __create_srdd(self):
        pass


@attr.s
class CircleRDD(SpatialRDD):

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self._srdd = self.__create_srdd()

    def __create_srdd(self):
        pass


@attr.s
class RectangleRDD(SpatialRDD):

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self._srdd = self.__create_srdd()

    def __create_srdd(self):
        pass


@attr.s
class LineStringRDD(SpatialRDD):

    def __create_srdd(self):
        pass

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        self._srdd = self.__create_srdd()