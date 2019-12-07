import attr

from geo_pyspark.core.jvm.abstract import JvmObject


@attr.s
class JvmCoordinates(JvmObject):
    x = attr.ib(default=0.0)
    y = attr.ib(default=0.0)

    def create_jvm_instance(self):
        return 1

    @property
    def jvm_reference(self):
        return "com.vividsolutions.jts.geom.Coordinate"


@attr.s
class JvmPoint(JvmObject):
    coordinate = attr.ib(type=JvmCoordinates)

    @property
    def jvm_object(self):
        return "com.vividsolutions.jts.geom.geometryFactory.createPoint"

    def create_jvm_instance(self):

        return "geometryFactory.createPoint(new Coordinate(-84.01, 34.01))"


@attr.s
class JvmEnvelope(JvmObject):
    minx = attr.ib()
    maxx = attr.ib()
    miny = attr.ib()
    maxy = attr.ib()

    def create_jvm_instance(self):
        envelope = self.get_reference()
        return envelope(self.minx, self.maxx, self.miny, self.maxy)

    @property
    def jvm_reference(self):
        return "com.vividsolutions.jts.geom.Envelope"
