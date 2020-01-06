from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD, JvmPolygonRDD, JvmRectangleRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geo_pyspark.core.enums.file_data_splitter import FileSplitterJvm, FileDataSplitter
from geo_pyspark.core.utils import JvmStorageLevel
from geo_pyspark.utils.meta import MultipleMeta


class PolygonRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self):
        session = SparkSession._instantiatedSession
        if session is None or session._sc._jsc is None:
            raise TypeError("Please initialize spark session")
        else:
            sc = session._sc
            super().__init__(sc)
            jvm_linestring_rdd = self._create_jvm_polygon_rdd(sc)
            srdd = jvm_linestring_rdd()
            self._srdd = srdd

    def __init__(self, jvmSpatialRDD: JvmPolygonRDD):
        """

        :param rawSpatialRDD:
        """
        super().__init__(jvmSpatialRDD.sc)
        self._srdd = jvmSpatialRDD.srdd

    def __init__(self, jvmSpatialRDD: JvmPolygonRDD, sourceEpsgCode: str, targetEpsgCode: str):
        """

        :param rawSpatialRDD:
        :param sourceEpsgCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """
        super().__init__(jvmSpatialRDD.sc)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(jvmSpatialRDD.sc)

        self._srdd = jvm_polygon_rdd(
            jvmSpatialRDD.srdd.getRawSpatialRDD(),
            sourceEpsgCode,
            targetEpsgCode
        )

    def __init__(self, jvmSpatialRDD: JvmPolygonRDD, newLevel: StorageLevel):
        """

        :param rawSpatialRDD:
        :param newLevel:
        """
        super().__init__(jvmSpatialRDD.sc)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(jvmSpatialRDD.sc)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            jvmSpatialRDD.srdd.getRawSpatialRDD(),
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        """

        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        """

        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
            splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
            splitter: FileDataSplitter, carryInputData: bool, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param newLevel:
        """

        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str,
            splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        :param newLevel:
        """

        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str,
            splitter: FileDataSplitter, carryInputData: bool, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            new_level_jvm
        )

    def __init__(self, jvmSpatialRDD: JvmPolygonRDD, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param rawSpatialRDD:
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """

        super().__init__(jvmSpatialRDD.sc)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)

        self._srdd = jvm_polygon_rdd(
            jvmSpatialRDD.srdd.getRawSpatialRDD(),
            newLevel,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: str,
            splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel,
                 sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """

        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            partitions,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
            splitter: FileDataSplitter, carryInputData: bool, newLevel: StorageLevel, sourceEpsgCRSCode: str,
            targetEpsgCode: str):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param startOffset:
        :param endOffset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                 partitions: int, newLevel: StorageLevel, sourceEpsgCRSCode: str,  targetEpsgCode: str):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter,
                 carryInputData: bool, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData: bool,
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def MinimumBoundingRectangle(self):
        from geo_pyspark.core.SpatialRDD import RectangleRDD
        rectangle_rdd = RectangleRDD()
        srdd = self._srdd.MinimumBoundingRectangle()

        rectangle_rdd.set_srdd(srdd)

        return rectangle_rdd

    @staticmethod
    def _create_jvm_polygon_rdd(sc: SparkContext):
        spatial_factory = SpatialRDDFactory(sc)

        jvm_polygon_rdd = spatial_factory.create_polygon_rdd()

        return jvm_polygon_rdd
