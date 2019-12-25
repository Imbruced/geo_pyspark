from typing import Optional

import attr
from pyspark import SparkContext


@attr.s
class FileSplitterJvm:

    sparkContext = attr.ib(type=SparkContext)

    def __attrs_post_init__(self):
        self._jvm = self.sparkContext._jvm

    def get_splitter(self, splitter: Optional[str]):
        return self.splitter(splitter) if splitter is not None else None

    @property
    def splitter(self):
        return self._jvm.FileDataSplitter.getFileDataSplitter


def get_geospark_package_location(jvm):
    return jvm.org.datasyslab.geospark