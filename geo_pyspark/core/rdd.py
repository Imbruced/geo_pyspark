from abc import ABC

import attr
from pyspark import SparkContext
from pyspark.sql import SparkSession

crs = str
path = str


@attr.s
class SpatialRDDFactory(ABC):

    sparkContext = attr.ib(type=SparkContext)

    def __attrs_post_init__(self):
        self._jsc = self.sparkContext._jsc
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
        self._srdd = self.sparkContext._jvm.GeometryRDDFactory.createPointRDD(
            self.InputLocation,
            self.Offset,
            self.splitter,
            self.carryInputData
        )

    def analyze(self) -> bool:
        raise NotImplementedError()

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
        raise NotImplementedError()

    def approximateTotalCount(self):
        raise NotImplementedError()

    def asInstanceOf(self):
        raise NotImplementedError()

    def boundary(self):
        raise NotImplementedError()

    def boundaryEnvelope(self):
        raise NotImplementedError()

    def buildIndex(self):
        raise NotImplementedError()

    def countWithoutDuplicates(self):
        raise NotImplementedError()

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
    pass


@attr.s
class PolygonRDD(SpatialRDD):
    pass


@attr.s
class CircleRDD(SpatialRDD):
    pass


@attr.s
class RectangleRDD(SpatialRDD):
    pass


@attr.s
class LineStringRDD(SpatialRDD):
    pass