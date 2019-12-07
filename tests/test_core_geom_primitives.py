import os

import pytest
from pyspark.sql import SparkSession

from geo_pyspark.core.jvm.geometry.primitive import JvmEnvelope
from geo_pyspark.register import upload_jars


upload_jars()

spark = SparkSession.builder.\
    master("local[*]").\
    getOrCreate()


class TestGeomPrimitives:

    def test_jvm_point(self):
        pass

    def test_jvm_envelope(self):
        envelope = JvmEnvelope(spark._jvm, 0.0, 5.0, 0.0, 5.0)
        jvm_instance = envelope.create_jvm_instance()
        envelope_area = jvm_instance.getArea()
        assert envelope_area == 25.0, f"Expected area to be equal 25 but {envelope_area} was found"

    def test_jvm_coordinates(self):
        pass
