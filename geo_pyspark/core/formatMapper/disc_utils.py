from enum import Enum

from pyspark import SparkContext

from geo_pyspark.core.SpatialRDD import SpatialRDD, PolygonRDD, LineStringRDD
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.decorators import require


class DiscLoader:

    @classmethod
    def load(cls, sc: SparkContext, path: str) -> SpatialRDD:
        raise NotImplementedError()


class IndexDiscLoader:

    @classmethod
    def load(cls, sc: SparkContext, path: str):
        jvm = sc._jvm
        index_rdd = jvm.ObjectSpatialRDDLoader.loadIndexRDD(sc._jsc, path)
        return index_rdd


class PolygonRDDDiscLoader(DiscLoader):

    @classmethod
    @require([GeoSparkLib.ObjectSpatialRDDLoader])
    def load(cls, sc: SparkContext, path: str) -> SpatialRDD:
        jvm = sc._jvm
        point_rdd = PolygonRDD()
        srdd = jvm.ObjectSpatialRDDLoader.loadPointSpatialRDD(sc._jsc, path)
        point_rdd.set_srdd(srdd)
        return point_rdd


class PointRDDDiscLoader(DiscLoader):

    @classmethod
    @require([GeoSparkLib])
    def load(cls, sc: SparkContext, path: str) -> SpatialRDD:
        jvm = sc._jvm
        polygon_rdd = PolygonRDD()
        srdd = jvm.ObjectSpatialRDDLoader.loadPolygonSpatialRDD(sc._jsc, path)
        polygon_rdd.set_srdd(srdd)
        return polygon_rdd


class LineStringRDDDiscLoader(DiscLoader):

    @classmethod
    @require([GeoSparkLib])
    def load(cls, sc: SparkContext, path: str) -> SpatialRDD:
        jvm = sc._jvm
        line_string_rdd = LineStringRDD()
        srdd = jvm.ObjectSpatialRDDLoader.loadLineStringSpatialRDD(sc._jsc, path)
        line_string_rdd.set_srdd(srdd)
        return line_string_rdd


class SpatialRDDDiscLoader(DiscLoader):

    @classmethod
    @require([GeoSparkLib])
    def load(cls, sc: SparkContext, path: str) -> SpatialRDD:
        jvm = sc._jvm
        spatial_rdd = SpatialRDD()
        srdd = jvm.ObjectSpatialRDDLoader.loadSpatialRDD(sc._jsc, path)
        spatial_rdd.set_srdd(srdd)
        return spatial_rdd


class GeometryType(Enum):
    POINT = "POINT"
    POLYGON = "POLYGON"
    LINESTRING = "LINESTRING"
    GEOMETRY = "GEOMETRY"


loaders = {
    GeometryType.POINT: PointRDDDiscLoader,
    GeometryType.POLYGON: PolygonRDDDiscLoader,
    GeometryType.LINESTRING: LineStringRDDDiscLoader,
    GeometryType.GEOMETRY: SpatialRDDDiscLoader
}


def load_spatial_rdd_from_disc(sc: SparkContext, path: str, geometry_type: GeometryType):
    """

    :param sc:
    :param path:
    :param geometry_type:
    :return:
    """
    return loaders[geometry_type].load(sc, path)


def load_spatial_index_rdd_from_disc(sc: SparkContext, path: str):

    return IndexDiscLoader.load(sc, path)