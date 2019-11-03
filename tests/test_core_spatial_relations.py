from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD import PointRDD, PolygonRDD
from geo_pyspark.core.enums import FileDataSplitter, GridType
from geo_pyspark.core.spatialOperator import JoinQuery
from geo_pyspark.register import upload_jars

upload_jars()


spark = SparkSession.\
    builder.\
    master("local").\
    getOrCreate()


class TestJoinQuery:

    def test_spatial_join_query(self):
        point_rdd = PointRDD(
            sparkContext=spark._sc,
            InputLocation="/home/pkocinski001/Desktop/projects/geo_pyspark_installed/points.csv",
            Offset=4,
            splitter=FileDataSplitter.WKT,
            carryInputData=True
        )

        polygon_rdd = PolygonRDD(
            sparkContext=spark._sc,
            InputLocation="/home/pkocinski001/Desktop/projects/geo_pyspark_installed/counties_tsv.csv",
            startingOffset=2,
            endingOffset=3,
            splitter=FileDataSplitter.WKT,
            carryInputData=True
        )

        point_rdd.analyze()
        point_rdd.spatialPartitioning(GridType.KDBTREE)
        polygon_rdd.spatialPartitioning(point_rdd.getPartitioner)
        result = JoinQuery.SpatialJoinQuery(
            point_rdd,
            polygon_rdd,
            True,
            False
        )

        print(result.count())