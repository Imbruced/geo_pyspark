import os

import pytest
from pyspark import StorageLevel

from geo_pyspark.core.SpatialRDD import PolygonRDD
from geo_pyspark.core.enums import IndexType, FileDataSplitter, GridType
from geo_pyspark.core.geom_types import Envelope
from tests.test_base import TestBase
from tests.utils import tests_path

inputLocation = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
queryWindowSet = os.path.join(tests_path, "resources/zcta510-small.csv")
offset = 0
splitter = FileDataSplitter.CSV
gridType = "rtree"
indexType = "rtree"
numPartitions = 5
distance = 0.01
queryPolygonSet = os.path.join(tests_path, "resources/primaryroads-polygon.csv")
inputLocationGeojson = os.path.join(tests_path, "resources/testPolygon.json")
inputLocationWkt = os.path.join(tests_path, "resources/county_small.tsv")
inputLocationWkb = os.path.join(tests_path, "resources/county_small_wkb.tsv")
inputCount = 3000
inputBoundary = Envelope(minx=-158.104182, maxx=-66.03575, miny=17.986328, maxy=48.645133)
containsMatchCount = 6941
containsMatchWithOriginalDuplicatesCount = 9334
intersectsMatchCount = 24323
intersectsMatchWithOriginalDuplicatesCount = 32726


class TestPolygonRDD(TestBase):

    def test_constructor(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()

        assert inputCount == spatial_rdd.approximateTotalCount
        assert inputBoundary == spatial_rdd.boundaryEnvelope

    def test_empty_constructor(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(gridType)
        spatial_rdd.buildIndex(IndexType.RTREE, True)
        spatial_rdd_copy = PolygonRDD()
        spatial_rdd_copy.rawSpatialRDD = spatial_rdd
        spatial_rdd_copy.analyze()

    def test_geojson_constructor(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=inputLocationGeojson,
            splitter=FileDataSplitter.GEOJSON,
            carryInputData=True,
            partitions=4,
            newLevel=StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.analyze()
        assert spatial_rdd.approximateTotalCount == 1001
        assert spatial_rdd.boundaryEnvelope is not None
        assert spatial_rdd.rawSpatialRDD.take(1)[0].getUserData() == "01\t077\t011501\t5\t1500000US010770115015\t010770115015\t5\tBG\t6844991\t32636"
        assert spatial_rdd.rawSpatialRDD.take(2)[1].getUserData() == "01\t045\t021102\t4\t1500000US010450211024\t010450211024\t4\tBG\t11360854\t0"
        assert spatial_rdd.fieldNames == ["STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE", "AFFGEOID", "GEOID", "NAME", "LSAD", "ALAND", "AWATER"]

    def test_wkt_constructor(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=inputLocationWkt,
            splitter=FileDataSplitter.WKT,
            carryInputData=True,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        assert spatial_rdd.approximateTotalCount == 103
        assert spatial_rdd.boundaryEnvelope is not None
        assert spatial_rdd.rawSpatialRDD.take(1)[0].getUserData() == "31\t039\t00835841\t31039\tCuming\tCuming County\t06\tH1\tG4020\t\t\t\tA\t1477895811\t10447360\t+41.9158651\t-096.7885168"

    def test_wkb_constructor(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=inputLocationWkb,
            splitter=FileDataSplitter.WKB,
            carryInputData=True,
            newLevel=StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.analyze()
        assert spatial_rdd.approximateTotalCount == 103
        assert spatial_rdd.boundaryEnvelope is not None
        assert spatial_rdd.rawSpatialRDD.take(1)[0].getUserData() == "31\t039\t00835841\t31039\tCuming\tCuming County\t06\tH1\tG4020\t\t\t\tA\t1477895811\t10447360\t+41.9158651\t-096.7885168"

    def test_hilbert_curve_spatial_partitioning(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.HILBERT)

        for envelope in spatial_rdd.grids:
            print(envelope)

    def test_r_tree_spatial_partitioning(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=inputLocation,
            splitter=splitter,
            carryInputData=True,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )

        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.RTREE)

        for envelope in spatial_rdd.grids:
            print(envelope)

    def test_voronoi_spatial_partitioning(self):
        spatial_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=inputLocation,
            splitter=FileDataSplitter.CSV,
            carryInputData=True,
            partitions=10,
            newLevel=StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.analyze()
        spatial_rdd.spatialPartitioning(GridType.VORONOI)

        for envelope in spatial_rdd.grids:
            print(envelope)

    def test_build_index_without_set_grid(self):
        spatial_rdd = PolygonRDD(
            self.sc,
            inputLocation,
            FileDataSplitter.CSV,
            carryInputData=True,
            partitions=numPartitions,
            newLevel=StorageLevel.MEMORY_ONLY
        )
        spatial_rdd.analyze()
        spatial_rdd.buildIndex(IndexType.RTREE, False)

    def test_build_rtree_index(self):
        pass
        # TODO add test

    def test_build_quad_tree_index(self):
        pass
        # TODO add test

    def test_mbr(self):
        polygon_rdd = PolygonRDD(
            sparkContext=self.sc,
            InputLocation=inputLocation,
            splitter=FileDataSplitter.CSV,
            carryInputData=True,
            partitions=numPartitions
        )

        rectangle_rdd = polygon_rdd.MinimumBoundingRectangle()

        result = rectangle_rdd.rawSpatialRDD.collect()

        for el in result:
            print(el.geom.wkt)
        print(result)
        assert result.__len__() > -1
