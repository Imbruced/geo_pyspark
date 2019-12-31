from pyspark import SparkContext, StorageLevel

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD, MultipleMeta
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geo_pyspark.core.enums.file_data_splitter import FileSplitterJvm, FileDataSplitter


class PolygonRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self):
        super().__init__()

    def __init__(self, rawSpatialRDD: SpatialRDD):
        """

        :param rawSpatialRDD:
        """
        super().__init__(rawSpatialRDD._sc)
        self._srdd = rawSpatialRDD._srdd

    def __init__(self, rawSpatialRDD: SpatialRDD, sourceEpsgCode: str, targetEpsgCode: str):
        """

        :param rawSpatialRDD:
        :param sourceEpsgCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """
        super().__init__(rawSpatialRDD._sc)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(rawSpatialRDD._sc)

        self._srdd = jvm_polygon_rdd(
            rawSpatialRDD._srdd.rawSpatialRDD(),
            sourceEpsgCode,
            targetEpsgCode
        )

    def __init__(self, rawSpatialRDD: SpatialRDD, newLevel: StorageLevel):
        """

        :param rawSpatialRDD:
        :param newLevel:
        """
        super().__init__(rawSpatialRDD._sc)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(rawSpatialRDD._sc)
        self._srdd = jvm_polygon_rdd(
            rawSpatialRDD._srdd.rawSpatialRDD(),
            newLevel
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

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            newLevel
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

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData,
            newLevel
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

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            newLevel
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

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            newLevel
        )

    def __init__(self, rawSpatialRDD: SpatialRDD, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param rawSpatialRDD:
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """

        super().__init__(rawSpatialRDD._sc)
        jvm_polygon_rdd = self._create_jvm_polygon_rdd(self._sc)

        self._srdd = jvm_polygon_rdd(
            rawSpatialRDD._srdd.rawSpatialRDD(),
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
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            newLevel,
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
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            startOffset,
            endOffset,
            jvm_splitter.jvm_instance,
            carryInputData,
            newLevel,
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

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            newLevel,
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

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            newLevel,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def MinimumBoundingRectangle(self):
        from geo_pyspark.core.SpatialRDD import RectangleRDD
        rectangle_rdd = RectangleRDD(
            spatialRDD=self,
            sparkContext=self._sc
        )
        return rectangle_rdd

    @staticmethod
    def _create_jvm_polygon_rdd(sc: SparkContext):
        spatial_factory = SpatialRDDFactory(sc)

        jvm_polygon_rdd = spatial_factory.create_polygon_rdd()

        return jvm_polygon_rdd
