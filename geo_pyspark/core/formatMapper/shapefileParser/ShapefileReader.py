import attr
from pyspark import SparkContext

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD


@attr.s
class ShapefileReader:

    @classmethod
    def readToGeometryRDD(cls, sc: SparkContext, shapefileInputLocation: str):
        shape_reader = cls._create_jsrdd(sc)
        jsrdd = shape_reader.readToGeometryRDD(
                sc._jsc,
                shapefileInputLocation
        )
        return SpatialRDD.from_srdd(sc, jsrdd)

    @classmethod
    def _create_jsrdd(cls, sc: SparkContext):
        return sc._jvm.org. \
            datasyslab. \
            geospark. \
            formatMapper. \
            shapefileParser. \
            ShapefileReader
