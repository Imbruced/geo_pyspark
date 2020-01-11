import os

from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.SpatialRDD import PolygonRDD
from tests.test_base import TestBase
from tests.utils import tests_path

point_path = os.path.join(tests_path, "resources/points.csv")
counties_path = os.path.join(tests_path, "resources/counties_tsv.csv")


class TestSpatialRDD(TestBase):

    def test_creating_point_rdd(self):
        point_rdd = PointRDD(
            self.spark._sc,
            point_path,
            4,
            FileDataSplitter.WKT,
            True
        )

        point_rdd.analyze()
        cnt = point_rdd.countWithoutDuplicates()
        assert cnt == 12872, f"Point RDD should have 12872 but found {cnt}"

    def test_creating_polygon_rdd(self):
        polygon_rdd = PolygonRDD(
            self.spark._sc,
            "/home/pkocinski001/Desktop/projects/geo_pyspark_installed/counties_tsv.csv",
            2,
            3,
            FileDataSplitter.WKT,
            True
        )

        polygon_rdd.analyze()

        cnt = polygon_rdd.countWithoutDuplicates()

        assert cnt == 407, f"Polygon RDD should have 407 but found {cnt}"
