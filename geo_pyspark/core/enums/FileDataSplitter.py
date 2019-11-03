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
