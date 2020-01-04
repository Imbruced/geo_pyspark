from typing import List

import attr
from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.utils.meta import MultipleMeta


class Adapter(metaclass=MultipleMeta):
    """
    Class which allow to convert between Spark DataFrame and SpatialRDD and reverse.
    """

    @classmethod
    def toSpatialRdd(cls, dataFrame: DataFrame, geometryFieldName: str) -> SpatialRDD:
        """

        :param dataFrame:
        :param geometryFieldName:
        :return:
        """
        pass

    @classmethod
    def toSpatialRdd(cls, dataFrame: DataFrame, geometryColId: int) -> SpatialRDD:
        """

        :param dataFrame:
        :param geometryColId:
        :return:
        """
        pass

    @classmethod
    def toSpatialRdd(cls, dataFrame: DataFrame, geometryColId: int, fieldNames: List[str]) -> SpatialRDD:
        """

        :param dataFrame:
        :param geometryColId:
        :param fieldNames:
        :return:
        """
        pass

    @classmethod
    def toSpatialRdd(cls, dataFrame: DataFrame, geometryFieldName: str, fieldNames: List[str]) -> SpatialRDD:
        """

        :param dataFrame:
        :param geometryFieldName:
        :param fieldNames:
        :return:
        """
        pass

    @classmethod
    def toDf(cls, spatialRDD: SpatialRDD, fieldNames: List[str], sparkSession: SparkSession) -> DataFrame:
        """

        :param spatialRDD:
        :param fieldNames:
        :param sparkSession:
        :return:
        """
        pass

    @classmethod
    def toDf(cls, spatialRDD: SpatialRDD, sparkSession: SparkSession) -> DataFrame:
        """

        :param spatialRDD:
        :param sparkSession:
        :return:
        """
        pass

    @classmethod
    def toDf(cls, spatialPairRDD: RDD, sparkSession: SparkSession):
        """

        :param spatialPairRDD:
        :param sparkSession:
        :return:
        """
        pass

    @classmethod
    def toDf(cls, spatialPairRDD: RDD, leftFieldnames: List[str], rightFieldNames: List[str], sparkSession: SparkSession):
        """

        :param spatialPairRDD:
        :param leftFieldnames:
        :param rightFieldNames:
        :param sparkSession:
        :return:
        """
        pass