import pytest
from pyspark.sql import SparkSession

from geo_pyspark.core.jvm.geometry.primitive import JvmPoint

spark = SparkSession.builder.\
    master("local[*]").\
    getOrCreate()


class TestGeomPrimitives:

    def test_jvm_point(self):
        pass

    def test_jvm_envelope(self):
        pass

    def test_jvm_coordinates(self):
        pass
