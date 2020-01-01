from pyspark import SparkContext, StorageLevel

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD, MultipleMeta
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geo_pyspark.core.enums.file_data_splitter import FileSplitterJvm, FileDataSplitter


class PointRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self):
        super().__init__()
        self._srdd = None

    def __init__(self, spatialRDD: SpatialRDD):
        """

        :param spatialRDD:
        """
        super().__init__()
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        self._srdd = jvm_point_rdd(spatialRDD._srdd)

    def __init__(self, spatialRDD: SpatialRDD, sourceEpsgCode: str, targetEpsgCode: str):
        """

        :param spatialRDD:
        :param sourceEpsgCode:
        :param targetEpsgCode:
        """
        super().__init__(spatialRDD._sc)
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        self._srdd = jvm_point_rdd(spatialRDD._srdd, sourceEpsgCode, targetEpsgCode)

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int):
        """

        :param sparkContext:
        :param InputLocation:
        :param Offset:
        :param splitter:
        :param carryInputData:
        :param partitions:
        """

        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = jvm_point_rdd(
            sparkContext._jsc,
            InputLocation,
            Offset,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int):
        """

        :param sparkContext:
        :param InputLocation:
        :param Offset:
        :param splitter:
        :param carryInputData:
        :param partitions:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = jvm_point_rdd(
            sparkContext._jsc,
            InputLocation,
            Offset,
            jvm_splitter,
            carryInputData,
            partitions
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
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
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
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
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
        super().__init__(spatialRDD._sc)
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        self._srdd = jvm_point_rdd(
            spatialRDD._srdd.rawSpatialRDD,
            newLevel
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int, newLevel: StorageLevel):
        """

        :param sparkContext:
        :param InputLocation:
        :param Offset:
        :param splitter:
        :param carryInputData:
        :param partitions:
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            Offset,
            jvm_splitter,
            carryInputData,
            partitions,
            newLevel
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, newLevel: StorageLevel):
        """

        :param sparkContext:
        :param InputLocation:
        :param Offset:
        :param splitter:
        :param carryInputData:
        :param newLevel:
        """

        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            Offset,
            splitter,
            carryInputData,
            newLevel
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, splitter: FileDataSplitter, carryInputData: bool,
                 partitions: int, newLevel: StorageLevel):
        """

        :param sparkContext:
        :param InputLocation:
        :param splitter:
        :param carryInputData:
        :param partitions:
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter,
            carryInputData,
            partitions,
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
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
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
        super().__init__(spatialRDD._sc)
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        self._srdd = jvm_point_rdd(
            spatialRDD._srdd.rawSpatialRDD(),
            newLevel,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, partitions: int, newLevel: StorageLevel, sourceEpsgCRSCode: str,
                 targetEpsgCode: str):

        """

        :param sparkContext:
        :param InputLocation:
        :param Offset:
        :param splitter:
        :param carryInputData:
        :param partitions:
        :param newLevel:
        :param sourceEpsgCRSCode:
        :param targetEpsgCode:
        """
        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            Offset,
            jvm_splitter,
            carryInputData,
            partitions,
            newLevel,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int, splitter: FileDataSplitter,
                 carryInputData: bool, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext:
        :param InputLocation:
        :param Offset:
        :param splitter:
        :param carryInputData:
        :param newLevel:
        :param sourceEpsgCRSCode:
        :param targetEpsgCode:
        """

        super().__init__(sparkContext)
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        self._srdd = jvm_point_rdd(
            self._jsc,
            InputLocation,
            Offset,
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
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
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
        jvm_point_rdd = self._create_jvm_point_rdd(self._sc)
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

    def MinimumBoundingRectangle(self):
        raise NotImplementedError("PointRDD has not MinimumBoundingRectangle method.")

    def _create_jvm_point_rdd(self, sc: SparkContext):
        spatial_factory = SpatialRDDFactory(sc)
        return spatial_factory.create_point_rdd()


point_rdd = PointRDD()

print(point_rdd.ie())