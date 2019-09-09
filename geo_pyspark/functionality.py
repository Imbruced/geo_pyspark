import os

from pyspark.sql import SparkSession

from geo_pyspark.register import GeoSparkRegistrator
print(os.environ)
# os.environ["PYSPARK_SUBMIT_ARGS"] = r"pyspark-shell --jars C:\Users\Pawel\Desktop\projects\geo_pyspark\geo_pyspark\jars\2_4 --master local[2] "
os.environ["PYSPARK_SUBMIT_ARGS"] += " --master local[2]"
print(os.environ["PYSPARK_SUBMIT_ARGS"])
        # += " --master local[2] pyspark-shell"
spark = SparkSession.builder. \
        master("local[2]").\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)

# spark.sql("SELECT 1").show()
print(os.environ)
spark.sql("SELECT st_geomFromWKT(21.0 52.0)").show()