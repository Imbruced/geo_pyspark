from typing import Optional, List

import attr
from py4j.java_gateway import get_field
from pyspark import SparkContext, RDD

from geo_pyspark.core.enums.grid_type import GridTypeJvm
from geo_pyspark.core.enums.index_type import IndexTypeJvm
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.utils.serde import GeoSparkPickler
from geo_pyspark.utils.types import crs


@attr.s
class SpatialRDD:

    sparkContext = attr.ib(type=Optional[SparkContext], default=None)

    def __attrs_post_init__(self):
        self._srdd = None
        if self.sparkContext is not None:
            self._jsc = self.sparkContext._jsc
            self._jvm = self.sparkContext._jvm
        else:
            self._jsc = None
            self._jvm = None

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
        raise NotImplementedError()

    @property
    def approximateTotalCount(self) -> int:
        """

        :return:
        """
        return get_field(self._srdd, "approximateTotalCount")

    def boundary(self):
        """

        :return:
        """

        jvm_boundary = self._srdd.boundary()
        envelope = Envelope.from_jvm_instance(jvm_boundary)
        return envelope

    @property
    def boundaryEnvelope(self):
        """

        :return:
        """
        java_boundary_envelope = get_field(self._srdd, "boundaryEnvelope")
        return Envelope.from_jvm_instance(java_boundary_envelope)

    def buildIndex(self, indexType: str, buildIndexOnSpatialPartitionedRDD: bool):
        """

        :param indexType:
        :param buildIndexOnSpatialPartitionedRDD:
        :return:
        """
        return self._srdd.buildIndex(
            IndexTypeJvm(self._jvm).get_index_type(indexType),
            buildIndexOnSpatialPartitionedRDD
        )

    def countWithoutDuplicates(self):
        """

        :return:
        """
        return self._srdd.countWithoutDuplicates()

    def countWithoutDuplicatesSPRDD(self):
        """

        :return:
        """
        return self._srdd.countWithoutDuplicatesSPRDD()

    @property
    def fieldNames(self):
        """

        :return:
        """
        try:
            field_names = list(get_field(self._srdd, "fieldNames"))
        except TypeError:
            field_names = []
        return field_names

    def getCRStransformation(self):
        """

        :return:
        """
        raise self.getCRSTransformation()

    @property
    def getPartitioner(self) -> str:
        """

        :return:
        """
        return self._srdd.getPartitioner()

    def getRawSpatialRDD(self):
        """

        :return:
        """
        serialized_spatial_rdd = self._jvm.GeoSerializerData.serializeToPython(self._srdd.getRawSpatialRDD())
        return RDD(serialized_spatial_rdd, self.sparkContext, GeoSparkPickler())

    def getSampleNumber(self):
        """

        :return:
        """
        return self._srdd.getSampleNumber()

    def getSourceEpsgCode(self) -> str:
        """
        Function which returns source EPSG code when it is assigned. If not an empty String is returned.
        :return:
        """
        return self._srdd.getSourceEpsgCode()

    def getTargetEpsgCode(self) -> str:
        """
        Function which returns target EPSG code when it is assigned. If not an empty String is returned.
        :return:
        """
        return self._srdd.getTargetEpgsgCode()

    @property
    def grids(self) -> List[Envelope]:
        """

        :return:
        """
        jvm_grids = get_field(self._srdd, "grids")
        number_of_grids = jvm_grids.size()

        envelopes = [Envelope.from_jvm_instance(jvm_grids[index]) for index in range(number_of_grids)]

        return envelopes

    @property
    def indexedRDD(self):
        """

        :return:
        """
        return self._srdd.indexedRDD()

    def indexedRawRDD(self):
        """

        :return:
        """
        return self._srdd.indexedRawRDD()

    def partitionTree(self):
        """

        :return:
        """
        raise self._srdd.partitionTree()

    @property
    def rawSpatialRDD(self):
        """

        :return:
        """
        return self.getRawSpatialRDD()

    @rawSpatialRDD.setter
    def rawSpatialRDD(self, spatial_rdd: 'SpatialRDD'):
        self._srdd = spatial_rdd._srdd
        self.sparkContext = spatial_rdd.sparkContext
        self._jvm = spatial_rdd._jvm

    def saveAsGeoJSON(self, path: str):
        """

        :param path:
        :return:
        """
        return self._srdd.saveAsGeoJSON(path)

    def saveAsWKB(self, path: str):
        """

        :param path:
        :return:
        """
        return self._srdd.saveAsWKB(path)

    def saveAsWKT(self, path: str):
        """

        :param path:
        :return:
        """
        return self._srdd.saveAsWKT(path)

    def setRawSpatialRDD(self):
        """

        :return:
        """
        raise self._setRawSpatialRDD()

    def setSampleNumber(self):
        """

        :return:
        """
        raise self._srdd.setSampleNumber()

    def spatialPartitionedRDD(self):
        """

        :return:
        """
        raise self._spatialPartitionedRDD()

    def spatialPartitioning(self, partitioning: str):
        """

        :param partitioning:
        :return:
        """
        if type(partitioning) == str:
            grid = GridTypeJvm(self._jvm)
            current_grid_type = grid.get_grid_type(partitioning)
        else:
            current_grid_type = partitioning
        return self._srdd.spatialPartitioning(
            current_grid_type
        )

    def set_srdd(self, srdd):
        self._srdd = srdd

