import attr

from geo_pyspark.core.SpatialRDD.abstract import AbstractSpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory


@attr.s
class CircleRDD(AbstractSpatialRDD):
    spatialRDD = attr.ib(default=None)
    radius = attr.ib(type=float, default=None)
    sparkContext = attr.ib(default=None)

    def __attrs_post_init__(self):
        self.sparkContext = self.spatialRDD.sparkContext
        super().__attrs_post_init__()

    def srdd_from_attributes(self):
        Spatial = SpatialRDDFactory(self.sparkContext).create_circle_rdd()
        return Spatial(self.spatialRDD._srdd, self.radius)
