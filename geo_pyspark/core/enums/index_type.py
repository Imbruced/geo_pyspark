import attr

from geo_pyspark.utils.decorators import classproperty


@attr.s
class IndexType:

    @classproperty
    def QUADTREE(self):
        return "QUADTREE"

    @classproperty
    def RTREE(self):
        return "RTREE"