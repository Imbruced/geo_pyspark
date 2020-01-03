from pyspark import SparkContext, StorageLevel

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD, JvmLineStringRDD, JvmRectangleRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.core.enums.file_data_splitter import FileSplitterJvm
from geo_pyspark.core.utils import JvmStorageLevel
from geo_pyspark.utils.meta import MultipleMeta


class LineStringRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self):
        super().__init__()
        self._srdd = None

    def __init__(self, jvmSpatialRDD: JvmLineStringRDD):
        """

        :param spatialRDD:
        """
        super().__init__()
        self._srdd = jvmSpatialRDD.srdd

    def __init__(self, jvmSpatialRDD: JvmLineStringRDD, sourceEpsgCode: str, targetEpsgCode: str):
        """

        :param spatialRDD:
        :param sourceEpsgCode:
        :param targetEpsgCode:
        """
        super().__init__(jvmSpatialRDD.sc)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        self._srdd = jvm_point_rdd(jvmSpatialRDD.srdd.getRawSpatialRDD(), sourceEpsgCode, targetEpsgCode)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter,  carryInputData: bool, partitions: int):
        """

        :param sparkContext:
        :param InputLocation:
        :param startOffset:
        :param endOffset:
        :param splitter:
        :param carryInputData:
        :param partitions:
        """

        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool):
        """

        :param sparkContext:
        :param InputLocation:
        :param startOffset:
        :param endOffset:
        :param splitter:
        :param carryInputData:
        """

        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                 partitions: int):
        """

        :param sparkContext:
        :param InputLocation:
        :param splitter:
        :param carryInputData:
        :param partitions:
        """

        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool):
        """

        :param sparkContext:
        :param InputLocation:
        :param splitter:
        :param carryInputData:
        """

        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData
        )

    def __init__(self, jvmSpatialRDD: JvmLineStringRDD, newLevel: StorageLevel):
        """

        :param spatialRDD:
        :param newLevel:
        """
        super().__init__(jvmSpatialRDD.sc)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        self._srdd = jvm_point_rdd(jvmSpatialRDD.srdd.getRawSpatialRDD(), new_level_jvm)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel):

        """

        :param sparkContext:
        :param InputLocation:
        :param startOffset:
        :param endOffset:
        :param splitter:
        :param carryInputData:
        :param partitions:
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            partitions,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, newLevel: StorageLevel):
        """

        :param sparkContext:
        :param InputLocation:
        :param startOffset:
        :param endOffset:
        :param splitter:
        :param carryInputData:
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int, newLevel: StorageLevel):
        """

        :param sparkContext:
        :param InputLocation:
        :param splitter:
        :param carryInputData:
        :param partitions:
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            partitions,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                newLevel: StorageLevel):
        """

        :param sparkContext:
        :param InputLocation:
        :param splitter:
        :param carryInputData:
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            new_level_jvm
        )

    def __init__(self, jvmSpatialRDD: JvmLineStringRDD, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param spatialRDD:
        :param newLevel:
        :param sourceEpsgCRSCode:
        :param targetEpsgCode:
        """
        super().__init__()
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_point_rdd(jvmSpatialRDD.srdd.getRawSpatialRDD(), new_level_jvm, sourceEpsgCRSCode, targetEpsgCode)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, startOffset: int, endOffset: int,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel,
                 sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext:
        :param InputLocation:
        :param startOffset:
        :param endOffset:
        :param splitter:
        :param carryInputData:
        :param partitions:
        :param newLevel:
        :param sourceEpsgCRSCode:
        :param targetEpsgCode:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance

        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_point_rdd(
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

        :param sparkContext:
        :param InputLocation:
        :param startOffset:
        :param endOffset:
        :param splitter:
        :param carryInputData:
        :param newLevel:
        :param sourceEpsgCRSCode:
        :param targetEpsgCode:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_point_rdd(
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
                 partitions: int, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext:
        :param InputLocation:
        :param splitter:
        :param carryInputData:
        :param partitions:
        :param newLevel:
        :param sourceEpsgCRSCode:
        :param targetEpsgCode:
        """

        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            partitions,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                 newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext:
        :param InputLocation:
        :param splitter:
        :param carryInputData:
        :param newLevel:
        :param sourceEpsgCRSCode:
        :param targetEpsgCode:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def _create_jvm_linestring_rdd(self, sc: SparkContext):
        spatial_factory = SpatialRDDFactory(sc)
        return spatial_factory.create_linestring_rdd()

    def MinimumBoundingRectangle(self):
        from geo_pyspark.core.SpatialRDD import RectangleRDD
        rectangle_rdd = RectangleRDD()
        rectangle_rdd.rawJvmSpatialRDD = JvmRectangleRDD(
            self._srdd.MinimumBoundingRectangle(),
            self._sc
        )
        return rectangle_rdd

    def getRawJvmSpatialRDD(self) -> JvmLineStringRDD:
        return JvmLineStringRDD(self._srdd, self._sc)
