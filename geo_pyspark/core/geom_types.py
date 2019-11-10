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
