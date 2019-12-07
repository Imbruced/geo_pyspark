import attr

from geo_pyspark.core.jvm.abstract import JvmObject


@attr.s
class JvmCoordinate(JvmObject):
    x = attr.ib(default=0.0)
    y = attr.ib(default=0.0)

    def create_jvm_instance(self):
        return self.jvm_reference(self.x, self.y)

    @property
    def jvm_reference(self):
        return self.jvm.org.imbruced.geo_pyspark.CoordinateFactory.createCoordinates


@attr.s
class JvmPoint(JvmObject):
    coordinate = attr.ib(type=JvmCoordinate)

    @property
    def jvm_reference(self):
        return self.jvm.org.imbruced.geo_pyspark.GeomFactory.createPoint

    def create_jvm_instance(self):

        return self.jvm_reference(self.coordinate)


@attr.s
class JvmEnvelope(JvmObject):
    minx = attr.ib()
    maxx = attr.ib()
    miny = attr.ib()
    maxy = attr.ib()

    def create_jvm_instance(self):
        envelope = self.jvm_reference
        return envelope(self.minx, self.maxx, self.miny, self.maxy)

    @property
    def jvm_reference(self):
        return self.jvm.com.vividsolutions.jts.geom.Envelope
