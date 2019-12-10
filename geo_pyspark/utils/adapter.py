from typing import List

import attr
from pyspark.sql import DataFrame, SparkSession

from geo_pyspark.core.SpatialRDD.abstract import AbstractSpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD


@attr.s
class Adapter:
    """
    Class which allow to convert between Spark DataFrame and SpatialRDD and reverse.
    """

    @classmethod
    def toDf(cls, spatialRDD, spark: SparkSession, field_names: List[str] = None) -> DataFrame:
        if field_names is None and isinstance(spatialRDD, AbstractSpatialRDD):
            df = spark._jvm.org.datasyslab.geosparksql.utils.Adapter.toDf(
                spatialRDD._srdd, spark._jsparkSession
            )
        else:
            df = spark._jvm.org.datasyslab.geosparksql.utils.Adapter.toDf(
                spatialRDD, spark._jsparkSession
            )
            return DataFrame(df, spark._jsparkSession.sqlContext())

    @classmethod
    def toRdd(cls, data_frame: DataFrame, geometry_col_id: int = None, geometry_field_name: str = None) -> DataFrame:
        pass

    @classmethod
    def toSpatialRdd(cls, df: DataFrame, geometry_field_name: str = None, geometry_col_id: str = None, field_names: List[str] = None) -> AbstractSpatialRDD:
        if geometry_field_name is not None:
            spatial_rdd = df._sc._jvm.org.datasyslab.geosparksql.utils.Adapter.toSpatialRdd(
                df._jdf, geometry_field_name
            )
            python_rdd = SpatialRDD(df._sc)
            python_rdd.set_srdd(spatial_rdd)
            return python_rdd