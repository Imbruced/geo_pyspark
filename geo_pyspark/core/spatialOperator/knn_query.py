import attr
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.geom_types import JvmCoordinate, JvmPoint
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.sql.geometry import GeometryFactory
from geo_pyspark.utils.binary_parser import BinaryParser
from geo_pyspark.utils.spatial_rdd_parser import SpatialRDDParserData


@attr.s
class KNNQuery:

    @classmethod
    @require([GeoSparkLib.KNNQuery, GeoSparkLib.GeometryAdapter])
    def SpatialKnnQuery(self, spatialRDD: SpatialRDD, originalQueryPoint: BaseGeometry, k: int,  useIndex: bool):
        """

        :param spatialRDD: spatialRDD
        :param originalQueryPoint: shapely.geometry.Point
        :param k: int
        :param useIndex: bool
        :return: pyspark.RDD
        """

        geom = GeometryFactory.to_bytes(originalQueryPoint)
        jvm = spatialRDD._jvm

        jvm_geom = jvm.GeometryAdapter.deserializeToGeometry(geom)

        knn_neighbours = jvm.KNNQuery.SpatialKnnQuery(spatialRDD._srdd, jvm_geom, k, useIndex)

        srdd = jvm.GeoSerializerData.serializeToPython(knn_neighbours)

        geoms_data = []
        for arr in srdd:
            binary_parser = BinaryParser(arr)
            geom = SpatialRDDParserData.deserialize(binary_parser)
            geoms_data.append(geom)

        return geoms_data
