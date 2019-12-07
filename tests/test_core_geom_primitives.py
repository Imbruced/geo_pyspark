import os

import pytest
from pyspark.sql import SparkSession

from geo_pyspark.core.jvm.geometry.primitive import JvmEnvelope, JvmCoordinate, JvmPoint
from geo_pyspark.register import upload_jars


upload_jars()

spark = SparkSession.builder.\
    master("local[*]").\
    getOrCreate()


class TestGeomPrimitives:

    def test_jvm_point(self):
        coordinate = JvmCoordinate(spark._jvm, 1.0, 1.0).create_jvm_instance()
        jvm_point = JvmPoint(spark._jvm, coordinate).create_jvm_instance()
        assert jvm_point.toString() == "POINT (1 1)"

    def test_jvm_envelope(self):
        envelope = JvmEnvelope(spark._jvm, 0.0, 5.0, 0.0, 5.0)
        jvm_instance = envelope.create_jvm_instance()
        envelope_area = jvm_instance.getArea()
        assert envelope_area == 25.0, f"Expected area to be equal 25 but {envelope_area} was found"

    def test_jvm_coordinates(self):
        coordinate = JvmCoordinate(spark._jvm, 1.0, 1.0).create_jvm_instance()
        assert coordinate.toString() == "(1.0, 1.0, NaN)", "Coordinate should has 1.0 as x"
