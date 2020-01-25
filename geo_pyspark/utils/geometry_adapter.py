from pyspark import SparkContext
from shapely.geometry.base import BaseGeometry

from geo_pyspark.core import Envelope
from geo_pyspark.sql.geometry import GeometryFactory


class GeometryAdapter:

    @classmethod
    def create_jvm_geometry_from_base_geometry(cls, jvm, geom: BaseGeometry):
        """
        :param jvm:
        :param geom:
        :return:
        """
        decoded_geom = GeometryFactory.to_bytes(geom)
        jvm_geom = jvm.GeometryAdapter.deserializeToGeometry(decoded_geom)
        if isinstance(geom, Envelope):
            jvm_geom = geom.create_jvm_instance(jvm)

        return jvm_geom
