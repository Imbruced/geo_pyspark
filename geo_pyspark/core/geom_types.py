import attr

from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.core.utils import require
from geo_pyspark.register.java_libs import GeoSparkLib


@attr.s
class JvmCoordinate(JvmObject):
    x = attr.ib(default=0.0)
    y = attr.ib(default=0.0)

    def _create_jvm_instance(self):
        return self.jvm.CoordinateFactory.createCoordinates(self.x, self.y)


@attr.s
class JvmPoint(JvmObject):
    coordinate = attr.ib(type=JvmCoordinate)

    def _create_jvm_instance(self):

        return self.jvm.GeomFactory.createPoint(self.coordinate)


@attr.s
class Envelope:
    minx = attr.ib()
    maxx = attr.ib()
    miny = attr.ib()
    maxy = attr.ib()

    @require([GeoSparkLib.Envelope])
    def create_jvm_instance(self, jvm):
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
