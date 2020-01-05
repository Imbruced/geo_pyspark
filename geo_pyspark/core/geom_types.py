import attr

from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib


@attr.s
class Envelope:
    minx = attr.ib()
    maxx = attr.ib()
    miny = attr.ib()
    maxy = attr.ib()

    @require([GeoSparkLib.Envelope])
    def create_java_object(self, jvm):
        return jvm.Envelope(
            self.minx, self.maxx, self.miny, self.maxy
        )

    @classmethod
    def from_jvm_instance(cls, java_obj):
        return cls(
            minx=java_obj.getMinX(),
            maxx=java_obj.getMaxX(),
            miny=java_obj.getMinY(),
            maxy=java_obj.getMaxY(),
        )

Envelope(1, 2, 3, 4)