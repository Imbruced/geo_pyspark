import os

import pytest
from pyspark.sql import SparkSession

from geo_pyspark.core.geom_types import JvmCoordinate, JvmPoint, Envelope
from geo_pyspark.register import upload_jars, GeoSparkRegistrator

upload_jars()

spark = SparkSession.builder.\
    master("local[*]").\
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestGeomPrimitives:

    def test_jvm_point(self):
        coordinate = JvmCoordinate(spark._jvm, 1.0, 1.0).jvm_instance
        jvm_point = JvmPoint(spark._jvm, coordinate).jvm_instance
        assert jvm_point.toString() == "POINT (1 1)"

    def test_jvm_envelope(self):
        envelope = Envelope(0.0, 5.0, 0.0, 5.0)
        jvm_instance = envelope.create_jvm_instance(spark.sparkContext._jvm)
        envelope_area = jvm_instance.getArea()
        assert envelope_area == 25.0, f"Expected area to be equal 25 but {envelope_area} was found"

    def test_jvm_coordinates(self):
        coordinate = JvmCoordinate(spark._jvm, 1.0, 1.0).jvm_instance
        assert coordinate.toString() == "(1.0, 1.0, NaN)", "Coordinate should has (1.0, 1.0, NaN) as string rep"
