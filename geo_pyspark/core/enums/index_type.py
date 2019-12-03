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


@attr.s
class IndexTypeJvm:

    jvm = attr.ib()

    def get_index_type(self, indexType: str):
        return self.jvm.org.\
            datasyslab.\
            geospark.\
            enums.\
            IndexType.\
            getIndexType(indexType)