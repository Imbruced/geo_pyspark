from typing import List

import attr
from pyspark.sql import DataFrame, SparkSession

from geo_pyspark.core.SpatialRDD.abstract import AbstractSpatialRDD


@attr.s
class Adapter:
    """
    Class which allow to convert between Spark DataFrame and SpatialRDD and reverse.
    """

    @classmethod
    def toDf(cls, spatialRDD: AbstractSpatialRDD, spark: SparkSession, field_names: List[str] = None) -> DataFrame:
        if field_names is None:
            df = spark._jvm.org.datasyslab.geosparksql.utils.Adapter.toDf(
                spatialRDD._srdd, spark._jsparkSession
            )
            return DataFrame(df, spark._jsparkSession.sqlContext())

    @classmethod
    def toRdd(cls, data_frame: DataFrame, geometry_col_id: int = None, geometry_field_name: str = None) -> DataFrame:
        pass

    @classmethod
    def toSpatialRdd(cls) -> AbstractSpatialRDD:
        pass