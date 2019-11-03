import attr
from pyspark import SparkContext


@attr.s
class FileSplitterJvm:

    sparkContext = attr.ib(type=SparkContext)

    def __attrs_post_init__(self):
        self._jvm = self.sparkContext._jvm

    def get_splitter(self, splitter: str):
        return self.splitter(splitter)

    @property
    def splitter(self):
        return self._jvm.org.datasyslab.\
            geospark.\
            enums.\
            FileDataSplitter.\
            getFileDataSplitter


def get_geospark_package_location(jvm):
    return jvm.org.datasyslab.geospark