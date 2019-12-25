import attr


@attr.s
class Envelope:
    minx = attr.ib()
    maxx = attr.ib()
    miny = attr.ib()
    maxy = attr.ib()

    def create_java_object(self, jvm):
        return jvm.com.vividsolutions.jts.geom.Envelope(
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
