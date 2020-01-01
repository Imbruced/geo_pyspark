from pyspark import SparkContext, StorageLevel

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD, MultipleMeta
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geo_pyspark.core.enums import FileDataSplitter
from geo_pyspark.core.enums.file_data_splitter import FileSplitterJvm


class LineStringRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self):
        self._srdd = None

    def __init__(self, spatialRDD: SpatialRDD):
        """

        :param spatialRDD:
        """
        super().__init__()
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        self._srdd = jvm_point_rdd(spatialRDD._srdd.rawSpatialRDD())

    def __init__(self, spatialRDD: SpatialRDD, sourceEpsgCode: str, targetEpsgCode: str):
        """

        :param spatialRDD:
        :param sourceEpsgCode:
        :param targetEpsgCode:
        """
        super().__init__(spatialRDD._sc)
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        self._srdd = jvm_point_rdd(spatialRDD._srdd.rawSpatialRDD(), sourceEpsgCode, targetEpsgCode)

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

    def __init__(self, spatialRDD: SpatialRDD, newLevel: StorageLevel):
        """

        :param spatialRDD:
        :param newLevel:
        """
        super().__init__()
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        self._srdd = jvm_point_rdd(spatialRDD._srdd.rawSpatialRDD(), newLevel)

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
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            partitions,
            newLevel
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
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            newLevel
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
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            partitions,
            jvm_splitter,
            newLevel
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
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            newLevel
        )

    def __init__(self, spatialRDD: SpatialRDD, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param spatialRDD:
        :param newLevel:
        :param sourceEpsgCRSCode:
        :param targetEpsgCode:
        """
        super().__init__()
        jvm_point_rdd = self._create_jvm_linestring_rdd(self._sc)
        self._srdd = jvm_point_rdd(spatialRDD._srdd.rawSpatialRDD(), newLevel, sourceEpsgCRSCode, targetEpsgCode)

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
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            partitions,
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
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter,
            carryInputData,
            newLevel,
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
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            partitions,
            newLevel,
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
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            newLevel,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def _create_jvm_linestring_rdd(self, sc: SparkContext):
        spatial_factory = SpatialRDDFactory(sc)
        return spatial_factory.create_linestring_rdd()

    def MinimumBoundingRectangle(self):
        from geo_pyspark.core.SpatialRDD import RectangleRDD
        rectangle_rdd = RectangleRDD(
            spatialRDD=self,
            sparkContext=self.sparkContext
        )
        return rectangle_rdd
