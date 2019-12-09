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
    def toDf(cls, spatialRDD: AbstractSpatialRDD, spark: SparkSession, field_names: List[str]) -> DataFrame:
        pass

    @classmethod
    def toRdd(cls, data_frame: DataFrame, geometry_col_id: int = None, geometry_field_name: str = None) -> DataFrame:
        pass

    @classmethod
    def toSpatialRdd(cls) -> AbstractSpatialRDD:
        pass