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
        sc = dataFrame._sc
        jvm = sc._jvm

        srdd = jvm.Adapter.toSpatialRdd(dataFrame._jdf, geometryFieldName)

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)

        return spatial_rdd


    @classmethod
    def toSpatialRdd(cls, dataFrame: DataFrame, geometryColId: int) -> SpatialRDD:
        """

        :param dataFrame:
        :param geometryColId:
        :return:
        """

        sc = dataFrame._sc
        jvm = sc._jvm

        srdd = jvm.Adapter.toSpatialRdd(dataFrame._jdf, geometryColId)

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)

        return spatial_rdd

    @classmethod
    def toSpatialRdd(cls, dataFrame: DataFrame, geometryColId: int, fieldNames: List[str]) -> SpatialRDD:
        """

        :param dataFrame:
        :param geometryColId:
        :param fieldNames:
        :return:
        """
        sc = dataFrame._sc
        jvm = sc._jvm

        srdd = jvm.Adapter.toSpatialRdd(dataFrame._jdf, geometryColId, fieldNames)

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)

        return spatial_rdd

    @classmethod
    def toSpatialRdd(cls, dataFrame: DataFrame, geometryFieldName: str, fieldNames: List[str]) -> SpatialRDD:
        """

        :param dataFrame:
        :param geometryFieldName:
        :param fieldNames:
        :return:
        """
        sc = dataFrame._sc
        jvm = sc._jvm

        srdd = jvm.Adapter.toSpatialRdd(dataFrame._jdf, geometryFieldName, fieldNames)

        spatial_rdd = SpatialRDD(sc)
        spatial_rdd.set_srdd(srdd)

        return spatial_rdd

    @classmethod
    def toDf(cls, spatialRDD: SpatialRDD, fieldNames: List[str], sparkSession: SparkSession) -> DataFrame:
        """

        :param spatialRDD:
        :param fieldNames:
        :param sparkSession:
        :return:
        """
        sc = spatialRDD._sc
        jvm = sc._jvm

        jdf = jvm.Adapter.toDf(spatialRDD._srdd, fieldNames, sparkSession._jsparkSession)

        df = DataFrame(jdf, sparkSession._wrapped)

        return df

    @classmethod
    def toDf(cls, spatialRDD: SpatialRDD, sparkSession: SparkSession) -> DataFrame:
        """

        :param spatialRDD:
        :param sparkSession:
        :return:
        """
        sc = spatialRDD._sc
        jvm = sc._jvm

        jdf = jvm.Adapter.toDf(spatialRDD._srdd, sparkSession._jsparkSession)

        df = DataFrame(jdf, sparkSession._wrapped)

        return df

    @classmethod
    def toDf(cls, spatialPairRDD: RDD, sparkSession: SparkSession):
        """

        :param spatialPairRDD:
        :param sparkSession:
        :return:
        """
        spatialPairRDD_mapped = spatialPairRDD.map(
            lambda x: [x[0].geom, *x[0].getUserData().split("\t"), x[1].geom, *x[1].getUserData().split("\t")]
        )
        df = sparkSession.createDataFrame(spatialPairRDD_mapped)
        return df

    @classmethod
    def toDf(cls, spatialPairRDD: RDD, leftFieldnames: List[str], rightFieldNames: List[str], sparkSession: SparkSession):
        """

        :param spatialPairRDD:
        :param leftFieldnames:
        :param rightFieldNames:
        :param sparkSession:
        :return:
        """
        sc = spatialPairRDD.ctx
        jvm = sc._jvm

        jdf = jvm.Adapter.toDf(spatialPairRDD._jrdd, leftFieldnames, rightFieldNames, sparkSession._jsparkSession)

        df = DataFrame(jdf, sparkSession._wrapped)

        return df