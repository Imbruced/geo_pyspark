import attr
from pyspark import SparkContext

from geo_pyspark.utils.decorators import classproperty


class GridType:

    @classproperty
    def EQUALGRID(self):
        return "EQUALGRID"

    @classproperty
    def HILBERT(self):
        return "HILBERT"

    @classproperty
    def RTREE(self):
        return "RTREE"

    @classproperty
    def VORONOI(self):
        return "VORONOI"

    @classproperty
    def QUADTREE(self):
        return "QUADTREE"

    @classproperty
    def KDBTREE(self):
        return "KDBTREE"


@attr.s
class GridTypeJvm:

    jvm = attr.ib()

    def get_grid_type(self, grid_type: str):
        return self.jvm.org.\
            datasyslab.\
            geospark.\
            enums.\
            GridType.\
            getGridType(grid_type)
