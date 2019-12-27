import pytest
from pyspark import StorageLevel
from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD import PolygonRDD
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.register import upload_jars, GeoSparkRegistrator

upload_jars()

spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()

GeoSparkRegistrator.\
    registerAll(spark)

sc = spark.sparkContext

inputLocation = "resources/primaryroads-polygon.csv"
queryWindowSet = "resources/zcta510-small.csv"
offset = 0
splitter = "csv"
gridType = "rtree"
indexType = "rtree"
numPartitions = 5
distance = 0.01
queryPolygonSet = "resources/primaryroads-polygon.csv"
inputLocationGeojson = "resources/testPolygon.json"
inputLocationWkt = "resources/county_small.tsv"
inputLocationWkb = "resources/county_small_wkb.tsv"
inputCount = 3000
inputBoundary = Envelope(minx=-158.104182, maxx=-66.03575, miny=17.986328, maxy=48.645133)
containsMatchCount = 6941
containsMatchWithOriginalDuplicatesCount = 9334
intersectsMatchCount = 24323
intersectsMatchWithOriginalDuplicatesCount = 32726


class TestPolygonRDD:

    def test_constructor(self):
        spatial_rdd = PolygonRDD(
            sparkContext=sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()

        assert inputCount == spatial_rdd.approximateTotalCount
        assert inputBoundary == spatial_rdd.boundaryEnvelope