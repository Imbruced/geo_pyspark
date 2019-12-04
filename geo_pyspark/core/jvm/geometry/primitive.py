import attr

from geo_pyspark.core.jvm.abstract import JvmObject


@attr.s
class JvmPoint(JvmObject):
    x = attr.ib(default=0.0)
    y = attr.ib(default=0.0)

    @property
    def jvm_object(self):
        return "com.vividsolutions.jts.geom.geometryFactory.createPoint"

    def create_jvm_instance(self):
        return "geometryFactory.createPoint(new Coordinate(-84.01, 34.01))"


@attr.s
class JvmCoordinates(JvmObject):
    x = attr.ib(default=0.0)
    y = attr.ib(default=0.0)

    @property
    def jvm_reference(self):
        return "com.vividsolutions.jts.geom.Coordinate"