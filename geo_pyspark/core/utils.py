from typing import Optional

import attr
from pyspark import SparkContext


@attr.s
class FileSplitterJvm:

    sparkContext = attr.ib(type=SparkContext)
    name = attr.ib(type=str)

    def __attrs_post_init__(self):
        self._jvm = self.sparkContext._jvm

    def get_splitter(self):
        return self.splitter(self.name) if self.name is not None else None

    @property
    def splitter(self):
        return self._jvm.FileDataSplitter.getFileDataSplitter
