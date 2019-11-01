from geo_pyspark.utils.decorators import classproperty


class FileDataSplitter:

    @classproperty
    def CSV(self):
        return ","

    @classproperty
    def TSV(self):
        return "\t"

    @classproperty
    def GEOJSON(self):
        return ""

    @classproperty
    def WKT(self):
        return "\t"

    @classproperty
    def WKB(self):
        return "\t"

    @classproperty
    def COMMA(self):
        return ","

    @classproperty
    def TAB(self):
        return "\t"

    @classproperty
    def QUESTIONMARK(self):
        return "?"

    @classproperty
    def SINGLEQUOTE(self):
        return "\'"

    @classproperty
    def QUOTE(self):
        return "\""

    @classproperty
    def UNSERSCORE(self):
        return "_"

    @classproperty
    def DASH(self):
        return "-"

    @classproperty
    def PERCENT(self):
        return "%"

    @classproperty
    def TILDE(self):
        return "~"

    @classproperty
    def PIPE(self):
        return "|"

    @classproperty
    def SEMICOLON(self):
        return ";"