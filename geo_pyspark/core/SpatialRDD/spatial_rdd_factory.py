from abc import ABC

import attr
from pyspark import SparkContext


@attr.s
class SpatialRDDFactory(ABC):

    sparkContext = attr.ib(type=SparkContext)

    def __attrs_post_init__(self):
        self._jvm = self.sparkContext._jvm

    def create_point_rdd(self):
        return self._jvm.PointRDD

    def create_polygon_rdd(self):
        return self._jvm.PolygonRDD

    def create_linestring_rdd(self):
        return self._jvm.LineStringRDD

    def create_rectangle_rdd(self):
        return self._jvm.RectangleRDD

    def create_circle_rdd(self):
        return self._jvm.CircleRDD
