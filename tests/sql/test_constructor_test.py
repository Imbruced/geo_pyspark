from tests.data import csv_point_input_location, area_lm_point_input_location, mixed_wkt_geometry_input_location, \
    mixed_wkb_geometry_input_location, geojson_input_location
from tests.test_base import TestBase


class TestConstructors(TestBase):

    def test_st_point(self):
        point_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").\
            load(csv_point_input_location)

        point_csv_df.createOrReplaceTempView("pointtable")

        point_df = self.spark.sql("select ST_Point(cast(pointtable._c0 as Decimal(24,20)), cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")
        assert point_df.count() == 1000

    def test_st_point_from_text(self):
        point_csv_df = self.spark.read.format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(area_lm_point_input_location)

        point_csv_df.createOrReplaceTempView("pointtable")
        point_csv_df.show(truncate=False)

        point_df = self.spark.sql("select ST_PointFromText(concat(_c0,',',_c1),',') as arealandmark from pointtable")
        assert point_df.count() == 121960

    def test_st_geom_from_wkt(self):
        polygon_wkt_df = self.spark.read.format("csv").\
          option("delimiter", "\t").\
          option("header", "false").\
          load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromWkt(polygontable._c0) as countyshape from polygontable")
        polygon_df.show(10)
        assert polygon_df.count() == 100

    def test_st_geom_from_text(self):
        polygon_wkt_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkt_geometry_input_location)

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_wkt_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromText(polygontable._c0) as countyshape from polygontable")
        polygon_df.show(10)
        assert polygon_df.count() == 100

    def test_st_geom_from_wkb(self):
        polygon_wkb_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(mixed_wkb_geometry_input_location)

        polygon_wkb_df.createOrReplaceTempView("polygontable")
        polygon_wkb_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromWKB(polygontable._c0) as countyshape from polygontable")
        polygon_df.show(10)
        assert polygon_df.count() == 100

    def test_st_geom_from_geojson(self):
        polygon_json_df = self.spark.read.format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(geojson_input_location)

        polygon_json_df.createOrReplaceTempView("polygontable")
        polygon_json_df.show()
        polygon_df = self.spark.sql("select ST_GeomFromGeoJSON(polygontable._c0) as countyshape from polygontable")
        polygon_df.show()
        assert polygon_df.count() == 1000
