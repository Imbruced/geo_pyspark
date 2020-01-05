import attr
from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD
from geo_pyspark.core.jvm.geometry.primitive import JvmCoordinate, JvmPoint
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.binary_parser import BinaryParser
from geo_pyspark.utils.spatial_rdd_parser import SpatialRDDParserData


@attr.s
class KNNQuery:

    @classmethod
    @require([GeoSparkLib.KNNQuery])
    def SpatialKnnQuery(self, spatialRDD: SpatialRDD, originalQueryPoint: Point, k: int,  useIndex: bool):
        """

        :param spatialRDD: spatialRDD
        :param originalQueryPoint: shapely.geometry.Point
        :param k: int
        :param useIndex: bool
        :return: pyspark.RDD
        """

        jvm = spatialRDD._jvm
        sc = spatialRDD._sc

        coordinate = JvmCoordinate(jvm, originalQueryPoint.x, originalQueryPoint.y)

        point = JvmPoint(spatialRDD._jvm, coordinate.jvm_instance)
        jvm_point = point.jvm_instance


        knn_neighbours = jvm.KNNQuery.SpatialKnnQuery(spatialRDD._srdd, jvm_point, k, useIndex)

        srdd = jvm.GeoSerializerData.serializeToPython(knn_neighbours)

        geoms_data = []
        for arr in srdd:
            binary_parser = BinaryParser(arr)
            geom = SpatialRDDParserData.deserialize(binary_parser)
            geoms_data.append(geom)

        return geoms_data
