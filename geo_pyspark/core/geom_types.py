from math import sqrt

import attr
from shapely.geometry import LineString, Point, Polygon
from shapely.geometry.base import BaseGeometry

from geo_pyspark.core.jvm.abstract import JvmObject
from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.decorators import require
from geo_pyspark.utils.meta import MultipleMeta


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
    minx = attr.ib(default=0)
    maxx = attr.ib(default=-1)
    miny = attr.ib(default=0)
    maxy = attr.ib(default=-1)

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

    def to_bytes(self):
        from geo_pyspark.utils.binary_parser import BinaryBuffer
        bin_buffer = BinaryBuffer()
        bin_buffer.put_double(self.minx)
        bin_buffer.put_double(self.maxx)
        bin_buffer.put_double(self.miny)
        bin_buffer.put_double(self.maxy)
        return bin_buffer.byte_array

    @classmethod
    def from_shapely_geom(cls, geometry: BaseGeometry):
        if isinstance(geometry, Point):
            return cls(geometry.x, geometry.x, geometry.y, geometry.y)
        else:
            envelope = geometry.envelope
            exteriors = envelope.exterior
            coordinates = list(exteriors.coords)
            x_coord = [coord[0] for coord in coordinates]
            y_coord = [coord[1] for coord in coordinates]

        return cls(min(x_coord), max(x_coord), min(y_coord), max(y_coord))


class Circle(metaclass=MultipleMeta):

    def __init__(self, centerGeometry: BaseGeometry, givenRadius: float):
        self.centerGeometry = centerGeometry
        self.radius = givenRadius
        center_geometry_mbr = Envelope.from_shapely_geom(self.centerGeometry)

        self.centerPoint = Point(
            (center_geometry_mbr.minx + center_geometry_mbr.maxx) / 2.0,
            (center_geometry_mbr.miny + center_geometry_mbr.maxy) / 2.0
        )

        width = center_geometry_mbr.maxx - center_geometry_mbr.minx
        length = center_geometry_mbr.maxy - center_geometry_mbr.miny

        center_geometry_internal_radius = sqrt(width ** 2 + length ** 2) / 2.0
        self.radius = givenRadius if givenRadius > center_geometry_internal_radius else center_geometry_internal_radius
        self.MBR = Envelope(
            self.centerPoint.x - self.radius,
            self.centerPoint.x + self.radius,
            self.centerPoint.y - self.radius,
            self.centerPoint.y + self.radius
        )

    def getCenterGeometry(self) -> BaseGeometry:
        pass

    def getCenterPoint(self):
        pass

    def getRadius(self) -> float:
        return self.radius

    def setRadius(self, givenRadius: float):
        center_geometry_mbr = Envelope.from_shapely_geom(self.centerGeometry)
        width = center_geometry_mbr.maxx - center_geometry_mbr.minx
        length = center_geometry_mbr.maxy - center_geometry_mbr.miny
        center_geometry_internal_radius = sqrt(width ** 2 + length ** 2) / 2
        self.radius = givenRadius if givenRadius > center_geometry_internal_radius else center_geometry_internal_radius
        self.MBR = Envelope(
            self.centerPoint.x - self.radius,
            self.centerPoint.x + self.radius,
            self.centerPoint.y - self.radius,
            self.centerPoint.y + self.radius
        )

    def covers(self, other: BaseGeometry) -> bool:
        pass

    def covers(self, lineString: LineString) -> bool:
        pass

    def covers(self, point: Point):
        pass

    def intersects(self, other: BaseGeometry):
        pass

    def intersects(self, polygon: Polygon) -> bool:
        pass

    def intersects(self, lineString: LineString) -> bool:
        pass

    def intersects(self, start: Point, end: Point) -> bool:
        pass

    @property
    def is_empty(self):
        pass

    def _compute_envelope_internal(self):
        if self.is_empty:
            return Envelope()
        return self.mbr

    def __str__(self):
        return "Circle of radius " + str(self.radius) + " around " + str(self.centerGeometry)

