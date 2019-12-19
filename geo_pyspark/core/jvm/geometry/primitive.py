import attr

from geo_pyspark.core.jvm.abstract import JvmObject


@attr.s
class JvmCoordinate(JvmObject):
    x = attr.ib(default=0.0)
    y = attr.ib(default=0.0)

    def _create_jvm_instance(self):
        return self.jvm.createCoordinates(self.x, self.y)


@attr.s
class JvmPoint(JvmObject):
    coordinate = attr.ib(type=JvmCoordinate)

    def _create_jvm_instance(self):

        return self.jvm.GeomFactory.createPoint(self.coordinate)


@attr.s
class JvmEnvelope(JvmObject):
    minx = attr.ib()
    maxx = attr.ib()
    miny = attr.ib()
    maxy = attr.ib()

    def _create_jvm_instance(self):
        return self.jvm.Envelope(self.minx, self.maxx, self.miny, self.maxy)
