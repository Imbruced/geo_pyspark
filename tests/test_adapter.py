import pytest
from pyspark.sql import SparkSession
from geo_pyspark.register import GeoSparkRegistrator
from geo_pyspark.register import upload_jars

upload_jars()

spark = SparkSession.\
    builder.\
    master("local[*]").\
    getOrCreate()

GeoSparkRegistrator.registerAll(spark)



class TestAdapter:

    def test_read_csv_point_into_spatial_rdd(self):
        pass

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
        pass

    def test_read_geojson_to_dataframe(self):
        pass

    def test_geojson_to_dataframe(self):
        import org.apache.spark.sql.functions.
        {callUDF, col}
        var
        spatialRDD = new
        PolygonRDD(sparkSession.sparkContext, geojsonInputLocation, FileDataSplitter.GEOJSON, true)
        spatialRDD.analyze()
        var
        df = Adapter.toDf(spatialRDD, sparkSession).withColumn("geometry", callUDF("ST_GeomFromWKT", col("geometry")))
        df.show()
        assert (df.columns(1) == "STATEFP")