import attr

from geo_pyspark.core.SpatialRDD.abstract import AbstractSpatialRDD


@attr.s
class SpatialRDD(AbstractSpatialRDD):

    def __attrs_post_init__(self):
        super().__attrs_post_init__()

    def srdd_from_attributes(self):
        raise NotImplementedError()
