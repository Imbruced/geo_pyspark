from abc import ABC

import attr
from pyspark import SparkContext


@attr.s
class SpatialRDDFactory(ABC):

    sparkContext = attr.ib(type=SparkContext)

    def __attrs_post_init__(self):
        self._jvm = self.sparkContext._jvm

    def create_point_rdd(self):
        return getattr(
            self._jvm,
            "org.datasyslab.geospark.spatialRDD.PointRDD"
        )

    def create_polygon_rdd(self):
        return getattr(
            self._jvm,
            "org.datasyslab.geospark.spatialRDD.PolygonRDD"
        )

    def create_linestring_rdd(self):
        return getattr(
            self._jvm,
            "org.datasyslab.geospark.spatialRDD.LineStringRDD"
        )

    def create_rectangle_rdd(self):
        return getattr(
            self._jvm,
            "org.datasyslab.geospark.spatialRDD.RectangleRDD"
        )

    def create_circle_rdd(self):
        return getattr(
            self._jvm,
            "org.datasyslab.geospark.spatialRDD.CircleRDD"
        )