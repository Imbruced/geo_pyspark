from pyspark.sql import SparkSession
from geo_pyspark.register import GeoSparkRegistrator

spark = SparkSession.builder.master("local[2]").getOrCreate()

GeoSparkRegistrator.registerAll(spark)

spark.sql("SELECT st_geomFromWKT('POINT(21.0 52.0)') as geom").printSchema()


