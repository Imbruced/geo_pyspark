import os

from pyspark.sql import SparkSession
from shapely.geometry import Point

from geo_pyspark.core.SpatialRDD import PointRDD, PolygonRDD, CircleRDD
from geo_pyspark.core.enums import GridType, FileDataSplitter, IndexType
from geo_pyspark.core.geom_types import Envelope
from geo_pyspark.core.spatialOperator import range_query, RangeQuery, KNNQuery, JoinQuery
from geo_pyspark.register import upload_jars

upload_jars()

spark = SparkSession.builder.\
    master("local[*]").\
    getOrCreate()

resource_folder = "data"

point_rdd_input_location = os.path.join(resource_folder, "arealm-small.csv")

point_rdd_splitter = FileDataSplitter.CSV

point_rdd_index_type = IndexType.RTREE
point_rdd_num_partitions = 5
point_rdd_offset = 1

polygon_rdd_input_location = os.path.join(resource_folder, "primaryroads-polygon.csv")
polygon_rdd_splitter = FileDataSplitter.CSV
polygon_rdd_index_type = IndexType.RTREE
polygon_rdd_num_partitions = 5
polygon_rdd_start_offset = 0
polygon_rdd_end_offset = 9


knn_query_point = Point(-84.01, 34.01)

range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)

join_query_partitionin_type = GridType.QUADTREE
each_query_loop_times = 1

sc = spark.sparkContext


class TestSpatialRDD:

    def test_empty_constructor_test(self):
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        object_rdd_copy = PointRDD()
        object_rdd_copy.rawSpatialRDD = object_rdd.rawSpatialRDD
        object_rdd_copy.analyze()

    def test_spatial_range_query(self):
        object_rdd = PointRDD(sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, False)
        for i in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                object_rdd, range_query_window, False, False
            ).count

    def test_range_query_using_index(self):
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        object_rdd.buildIndex(point_rdd_index_type, False)
        for i in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                object_rdd, range_query_window, False, True).count

    def test_knn_query(self):
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        for i in range(each_query_loop_times):
            result = KNNQuery.SpatialKnnQuery(object_rdd, knn_query_point, 1000, False)

    def test_knn_query_with_index(self):
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        object_rdd.buildIndex(point_rdd_index_type, False)
        for i  in range(each_query_loop_times):
            result = KNNQuery.SpatialKnnQuery(object_rdd, knn_query_point, 1000, True)

    def test_spaltial_join(self):
        query_window_rdd = PolygonRDD(
            sparkContext=sc,
            InputLocation=polygon_rdd_input_location,
            startingOffset=polygon_rdd_start_offset,
            endingOffset=polygon_rdd_end_offset,
            splitter=polygon_rdd_splitter,
            carryInputData=True
        )

        object_rdd = PointRDD(
            sparkContext=sc,
            InputLocation=point_rdd_input_location,
            Offset=point_rdd_offset,
            splitter=point_rdd_splitter,
            carryInputData=False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner)

        for x in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd, query_window_rdd, False, True).count

    def test_spatial_join_using_index(self):
        query_window = PolygonRDD(
            sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window.spatialPartitioning(object_rdd.getPartitioner)

        object_rdd.buildIndex(point_rdd_index_type, True)

        for i in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd, query_window, True, False).count()

    def test_spatial_join_using_index_on_polygons(self):
        query_window = PolygonRDD(
            sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window.spatialPartitioning(object_rdd.getPartitioner)

        query_window.buildIndex(polygon_rdd_index_type, True)

        for i in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd,
                query_window,
                True,
                False
            ).count()

    def test_spatial_join_query_using_index_on_polygons(self):
        query_window_rdd = PolygonRDD(
            sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner)

        for i in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd, query_window_rdd, True, False
            )

    def test_spatial_join_query_and_build_index_on_points_on_the_fly(self):
        query_window = PolygonRDD(
            sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window.spatialPartitioning(object_rdd.getPartitioner)

        for i in range(each_query_loop_times):
            result_size = JoinQuery.SpatialJoinQuery(
                object_rdd,
                query_window,
                True,
                False
            ).count()

    def test_spatial_join_query_and_build_index_on_polygons_on_the_fly(self):
        query_window_rdd = PolygonRDD(
            sc,
            polygon_rdd_input_location,
            polygon_rdd_start_offset,
            polygon_rdd_end_offset,
            polygon_rdd_splitter,
            True
        )

        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        object_rdd.analyze()
        object_rdd.spatialPartitioning(join_query_partitionin_type)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner)

        for i in range(each_query_loop_times):
            join_params = JoinParams(False, polygon_rdd_index_type, JoinBuildSide.LEFT)
            resultSize = JoinQuery.spatialJoin(
                query_window_rdd,
                object_rdd,
                join_params
            ).count()

    def test_distance_join_query(self):
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        query_window_rdd = CircleRDD(object_rdd, 0.1)
        object_rdd.analyze()
        object_rdd.spatialPartitioning(GridType.QUADTREE)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner)

        for i in range(each_query_loop_times):
            result_size = JoinQuery.DistanceJoinQuery(
                object_rdd,
                query_window_rdd,
                False,
                True).count()

    def test_distance_join_query_using_index(self):
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False
        )
        query_window_rdd = CircleRDD(object_rdd, 0.1)
        object_rdd.analyze()
        object_rdd.spatialPartitioning(GridType.QUADTREE)
        query_window_rdd.spatialPartitioning(object_rdd.getPartitioner)

        object_rdd.buildIndex(IndexType.RTREE, True)

        for i in range(each_query_loop_times):
            result_size = JoinQuery.DistanceJoinQuery(
                object_rdd,
                query_window_rdd,
                True,
                True
            ).count

    def test_earthdata_format_mapper(self):
        input_location = "test/data/modis/modis.csv"
        splitter = FileDataSplitter.CSV
        index_type = IndexType.RTREE
        query_envelope = Envelope(-90.01, -80.01, 30.01, 40.01)
        num_partitions = 5
        loop_times = 1
        hdf_increment = 5
        hdf_offset = 2
        hdf_root_group_name = "MOD_Swath_LST"
        hdf_data_variable_name = "LST"
        url_prefix = "test/resources/modis/"
        hdf_daya_variable_list = ["LST", "QC", "Error_LST", "Emis_31", "Emis_32"]

        earth_data_hdf_point = EarthdataHDFPointMapper(
            hdf_increment, hdf_offset, hdf_root_group_name,
            hdf_daya_variable_list, hdf_data_variable_name, url_prefix)
        spatial_rdd = PointRDD(
            sc,
            input_location,
            num_partitions,
            earth_data_hdf_point)

        i = 0
        while i < loop_times:
            result_size = 0
            result_size = RangeQuery.SpatialRangeQuery(
                spatial_rdd,
                query_envelope,
                False,
                False
            ).count
            i = i + 1

    def test_crs_transformed_spatial_range_query(self):
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False,
            StorageLevel.NONE,
            "epsg:4326",
            "epsg:3005"
        )
        for i in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                object_rdd, range_query_window, False, False
            )

    def test_crs_tranformed_spatial_range_query_using_index(self):
        object_rdd = PointRDD(
            sc,
            point_rdd_input_location,
            point_rdd_offset,
            point_rdd_splitter,
            False,
            StorageLevel.NONE,
            "epsg:4326",
            "epsg:3005"
        )
        object_rdd.buildIndex(point_rdd_index_type, False)
        for i in range(each_query_loop_times):
            result_size = RangeQuery.SpatialRangeQuery(
                object_rdd,
                range_query_window,
                False,
                True
            ).count
