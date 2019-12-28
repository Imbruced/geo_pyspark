import pytest

from geo_pyspark.core.SpatialRDD import PolygonRDD, PointRDD


class TestJoinQueryCorrectness:

    def test_inside_point_join_correctness(self):
        window_rdd = PolygonRDD(
            sc.parallelize(this.testPolygonWindowSet), StorageLevel.MEMORY_ONLY()
        )
        object_rdd = PointRDD(
            sc.parallelize(this.testInsidePointSet), StorageLevel.MEMORY_ONLY()
        );
        prepareRDDs(objectRDD, windowRDD);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, true, false).collect();
        verifyJoinResults(result);

        List<Tuple2<Polygon, HashSet<Point>>> resultNoIndex = JoinQuery.SpatialJoinQuery(objectRDD, windowRDD, false, false).collect();
        verifyJoinResults(resultNoIndex);

    def