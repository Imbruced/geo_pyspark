import attr
from pyspark import SparkContext

from geo_pyspark.utils.decorators import classproperty


class FileDataSplitter:

    @classproperty
    def CSV(self):
        return "CSV"

    @classproperty
    def TSV(self):
        return "TSV"

    @classproperty
    def GEOJSON(self):
        return "GEOJSON"

    @classproperty
    def WKT(self):
        return "WKT"

    @classproperty
    def WKB(self):
        return "WKB"

    @classproperty
    def COMMA(self):
        return "COMMA"

    @classproperty
    def TAB(self):
        return "TAB"

    @classproperty
    def QUESTIONMARK(self):
        return "QUESTIONMARK"

    @classproperty
    def SINGLEQUOTE(self):
        return "SINGLEQUOTE"

    @classproperty
    def QUOTE(self):
        return "QUOTE"

    @classproperty
    def UNDERSCORE(self):
        return "UNDERSCORE"

    @classproperty
    def DASH(self):
        return "DASH"

    @classproperty
    def PERCENT(self):
        return "PERCENT"

    @classproperty
    def TILDE(self):
        return "TILDE"

    @classproperty
    def PIPE(self):
        return "PIPE"

    @classproperty
    def SEMICOLON(self):
        return "SEMICOLON"


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