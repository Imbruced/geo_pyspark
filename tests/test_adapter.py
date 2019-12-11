import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

from geo_pyspark.core.SpatialRDD import PolygonRDD, CircleRDD
from geo_pyspark.core.enums import FileDataSplitter, GridType, IndexType
from geo_pyspark.core.formatMapper.shapefileParser.shape_file_reader import ShapefileReader
from geo_pyspark.core.spatialOperator import JoinQuery
from geo_pyspark.register import GeoSparkRegistrator
from geo_pyspark.register import upload_jars
from geo_pyspark.utils.adapter import Adapter
from tests.data import geojson_input_location, shape_file_with_missing_trailing_input_location
from tests.data import shape_file_input_location, area_lm_point_input_location
from tests.data import mixed_wkt_geometry_input_location

from pyspark import RDD

upload_jars()

spark = SparkSession.\
    builder.\
    master("local[*]").\
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)


class TestAdapter:

    def test_read_csv_point_into_spatial_rdd(self):
        df = spark.read.\
            format("csv").\
            option("delimiter", "\t").\
            option("header", "false").\
            load(area_lm_point_input_location)

        df.show()
        df.createOrReplaceTempView("inputtable")

        spatial_df = spark.sql("select ST_PointFromText(inputtable._c0,\",\") as arealandmark from inputtable")
        spatial_df.show()
        spatial_df.printSchema()

        spatial_rdd = Adapter.toSpatialRdd(spatial_df, "arealandmark")
        spatial_rdd.analyze()
        Adapter.toDf(spatial_rdd, spark).show()

    def test_csv_point_at_different_column_id_into_spatial_rdd(self):
        pass

    def test_read_csv_point_at_a_different_column_col_name_into_a_spatial_rdd(self):
        pass

    def test_read_csv_point_into_a_spatial_rdd_by_passing_coordinates(self):
        pass

    def test_read_csv_point_into_s_spatial_rdd_with_unique_id_by_passing_coordinates(self):
        pass

    def test_read_mixed_wkt_geometries_into_spatial_rdd(self):
        pass

    def test_read_mixed_wkt_geometries_into_spatial_rdd_with_unique_id(self):
        pass

    def test_read_shapefile_to_dataframe(self):
        spatial_rdd = ShapefileReader.readToGeometryRDD(
            spark.sparkContext, shape_file_input_location)
        spatial_rdd.analyze()
        logging.info(spatial_rdd.fieldNames)
        df = Adapter.toDf(spatial_rdd, spark)
        df.show()

    def test_read_shapefile_with_missing_to_dataframe(self):
        spatial_rdd = ShapefileReader.\
            readToGeometryRDD(spark.sparkContext, shape_file_with_missing_trailing_input_location)

        spatial_rdd.analyze()
        logging.info(spatial_rdd.fieldNames)

        df = Adapter.toDf(spatial_rdd, spark)
        df.show()

    def test_geojson_to_dataframe(self):
        spatial_rdd = PolygonRDD(
            spark.sparkContext, geojson_input_location, FileDataSplitter.GEOJSON, True
        )

        spatial_rdd.analyze()

        df = Adapter.toDf(spatial_rdd, spark).\
            withColumn("geometry", expr("ST_GeomFromWKT(geometry)"))
        df.show()
        assert (df.columns[1] == "STATEFP")

    def test_distance_join_result_to_dataframe(self):
        point_csv_df = spark.\
            read.\
            format("csv").\
            option("delimiter", ",").\
            option("header", "false").load(
                area_lm_point_input_location
        )
        point_csv_df.createOrReplaceTempView("pointtable")
        point_df = spark.sql(
            "select ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as arealandmark from pointtable")

        point_rdd = Adapter.toSpatialRdd(point_df, "arealandmark")
        point_rdd.analyze()

        polygon_wkt_df = spark.read.\
            format("csv").\
            option("delimiter", "\t").\
            option("header", "false").load(
                mixed_wkt_geometry_input_location
        )

        polygon_wkt_df.createOrReplaceTempView("polygontable")
        polygon_df = spark.\
            sql("select ST_GeomFromWKT(polygontable._c0) as usacounty from polygontable")

        polygon_rdd = Adapter.toSpatialRdd(polygon_df, "usacounty")
        polygon_rdd.analyze()
        circle_rdd = CircleRDD(polygon_rdd, 0.2)

        point_rdd.spatialPartitioning(GridType.QUADTREE)
        circle_rdd.spatialPartitioning(point_rdd.getPartitioner)

        point_rdd.buildIndex(IndexType.QUADTREE, True)

        join_result_pair_rdd = JoinQuery.\
            DistanceJoinQueryFlat(point_rdd, circle_rdd, True, True)
        #
        print("S")
        join_result_df = Adapter.toDf(join_result_pair_rdd, spark)
        join_result_df.printSchema()
        join_result_df.show()

