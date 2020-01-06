from pyspark import SparkContext, StorageLevel
from pyspark.sql import SparkSession

from geo_pyspark.core.SpatialRDD.spatial_rdd import SpatialRDD, JvmRectangleRDD, JvmSpatialRDD
from geo_pyspark.core.SpatialRDD.spatial_rdd_factory import SpatialRDDFactory
from geo_pyspark.core.enums.file_data_splitter import FileSplitterJvm, FileDataSplitter
from geo_pyspark.core.utils import JvmStorageLevel
from geo_pyspark.utils.meta import MultipleMeta


class RectangleRDD(SpatialRDD, metaclass=MultipleMeta):

    def __init__(self):
        session = SparkSession._instantiatedSession
        if session is None or session._sc._jsc is None:
            raise TypeError("Please initialize spark session")
        else:
            sc = session._sc
            super().__init__(sc)
            jvm_linestring_rdd = self._create_jvm_rectangle_rdd(sc)
            srdd = jvm_linestring_rdd()
            self._srdd = srdd

    def __init__(self, rawSpatialRDD: JvmRectangleRDD):
        """

        :param rawSpatialRDD:
        """
        super().__init__(rawSpatialRDD.sc)
        self._srdd = rawSpatialRDD.srdd

    def __init__(self, rawSpatialRDD: JvmRectangleRDD, sourceEpsgCode: str, targetEpsgCode: str):
        """

        :param rawSpatialRDD:
        :param sourceEpsgCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """
        super().__init__(rawSpatialRDD.sc)
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)

        self._srdd = jvm_polygon_rdd(
            rawSpatialRDD.srdd.getRawSpatialRDD(),
            sourceEpsgCode,
            targetEpsgCode
        )

    def __init__(self, rawSpatialRDD: JvmRectangleRDD, newLevel: StorageLevel):
        """

        :param rawSpatialRDD:
        :param newLevel:
        """
        super().__init__(rawSpatialRDD.sc)
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            rawSpatialRDD.srdd.getRawSpatialRDD(),
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int,
                 splitter: FileDataSplitter, carryInputData: bool, partitions: int):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param Offset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            Offset,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int,
                 splitter: FileDataSplitter, carryInputData: bool):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param Offset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            Offset,
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
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
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
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int,
            splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param Offset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        :param newLevel:
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            Offset,
            jvm_splitter.jvm_instance,
            carryInputData,
            partitions,
            new_level_jvm
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int,
            splitter: FileDataSplitter, carryInputData: bool, newLevel: StorageLevel):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param Offset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param newLevel:
        """

        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            Offset,
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
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
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
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            jvm_splitter.jvm_instance,
            carryInputData,
            new_level_jvm
        )

    def __init__(self, rawSpatialRDD: JvmSpatialRDD, newLevel: StorageLevel, sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param rawSpatialRDD:
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """

        super().__init__(rawSpatialRDD.sc)
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)

        self._srdd = jvm_polygon_rdd(
            rawSpatialRDD.srdd.getRawSpatialRDD(),
            newLevel,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int,
            splitter: FileDataSplitter, carryInputData: bool, partitions: int, newLevel: StorageLevel,
                 sourceEpsgCRSCode: str, targetEpsgCode: str):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param Offset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param partitions: int, the partitions
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """

        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter).jvm_instance
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            Offset,
            jvm_splitter,
            carryInputData,
            partitions,
            new_level_jvm,
            sourceEpsgCRSCode,
            targetEpsgCode
        )

    def __init__(self, sparkContext: SparkContext, InputLocation: str, Offset: int,
            splitter: FileDataSplitter, carryInputData: bool, newLevel: StorageLevel, sourceEpsgCRSCode: str,
            targetEpsgCode: str):
        """

        :param sparkContext: SparkContext, the spark context
        :param InputLocation: str, the input location
        :param Offset:
        :param splitter: FileDataSplitter, File data splitter which should be used to split the data
        :param carryInputData:
        :param newLevel:
        :param sourceEpsgCRSCode: str, the source epsg CRS code
        :param targetEpsgCode: str, the target epsg code
        """
        super().__init__(sparkContext)
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
        jvm_splitter = FileSplitterJvm(self._jvm, splitter)
        new_level_jvm = JvmStorageLevel(self._jvm, newLevel).jvm_instance

        self._srdd = jvm_polygon_rdd(
            self._jsc,
            InputLocation,
            Offset,
            jvm_splitter.jvm_instance,
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
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
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
        jvm_polygon_rdd = self._create_jvm_rectangle_rdd(self._sc)
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

    @staticmethod
    def _create_jvm_rectangle_rdd(sc: SparkContext):
        spatial_factory = SpatialRDDFactory(sc)

        jvm_polygon_rdd = spatial_factory.create_rectangle_rdd()

        return jvm_polygon_rdd

    def MinimumBoundingRectangle(self):
        raise NotImplementedError("RectangleRDD has not MinimumBoundingRectangle method.")

    def getRawJvmSpatialRDD(self) -> JvmRectangleRDD:
        return JvmRectangleRDD(self._srdd, self._sc)

    @property
    def rawJvmSpatialRDD(self) -> JvmRectangleRDD:
        return self.getRawJvmSpatialRDD()