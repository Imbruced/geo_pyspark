from enum import Enum


class GeoSparkLib(Enum):
    JoinParams = "org.imbruced.geo_pyspark.JoinParams"
    Adapter = "org.datasyslab.geosparksql.utils.Adapter"
    GeoSparkWrapper = "org.imbruced.geo_pyspark.GeoSparkWrapper"
    JoinQuery = "org.datasyslab.geospark.spatialOperator.JoinQuery"
    KNNQuery = "org.datasyslab.geospark.spatialOperator.KNNQuery"
    CoordinateFactory = "org.imbruced.geo_pyspark.CoordinateFactory"
    RangeQuery = "org.datasyslab.geospark.spatialOperator.RangeQuery"
    GeomFactory = "org.imbruced.geo_pyspark.GeomFactory"
    Envelope = "com.vividsolutions.jts.geom.Envelope"
    GeoSerializerData = "org.imbruced.geo_pyspark.serializers.GeoSerializerData"
    PointRDD = "org.datasyslab.geospark.spatialRDD.PointRDD"
    PolygonRDD = "org.datasyslab.geospark.spatialRDD.PolygonRDD"
    CircleRDD = "org.datasyslab.geospark.spatialRDD.CircleRDD"
    LineStringRDD = "org.datasyslab.geospark.spatialRDD.LineStringRDD"
    RectangleRDD = "org.datasyslab.geospark.spatialRDD.RectangleRDD"
    FileDataSplitter = "org.datasyslab.geospark.enums.FileDataSplitter"
    GeoJsonReader = "org.datasyslab.geospark.formatMapper.GeoJsonReader"
    ShapeFileReader = "org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader"
    GeoSparkSQLRegistrator = "org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator"
