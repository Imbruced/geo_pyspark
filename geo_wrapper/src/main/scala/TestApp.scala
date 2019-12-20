import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, GeometryFactory, Point, Polygon}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{PointRDD, PolygonRDD}
import org.datasyslab.geosparksql.utils.GeometrySerializer

object TestApp extends App{
//
//  object GeoSerializer {
//    def serialize(spatialRDD: JavaRDD[Polygon]): JavaRDD[String] = {
//      spatialRDD.map(new org.apache.spark.api.java.function.Function[Polygon, String]() {
//        override def call(polygon: Polygon): String = polygon.toText
//      })
//    }
//
//  }


  val resourceFolder = "/home/pawel/Desktop/projects/GeoSpark/core" + "/src/test/resources/"

  val PointRDDInputLocation = resourceFolder + "arealm-small.csv"
  val PointRDDSplitter = FileDataSplitter.CSV
  val PointRDDIndexType = IndexType.RTREE
  val PointRDDNumPartitions = 5
  val PointRDDOffset = 1

  val PolygonRDDInputLocation = "/home/pawel/Desktop/projects/GeoSpark/sql/src/test/resources/testPolygon.json"
  val PolygonRDDSplitter = FileDataSplitter.CSV
  val PolygonRDDIndexType = IndexType.RTREE
  val PolygonRDDNumPartitions = 5
  val PolygonRDDStartOffset = 0
  val PolygonRDDEndOffset = 9

  val geometryFactory = new GeometryFactory()
  val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
  val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
  val joinQueryPartitioningType = GridType.QUADTREE
  val eachQueryLoopTimes = 1
  val spark = SparkSession.
    builder().
    master("local").
    getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val queryWindowRDD = new PolygonRDD(spark.sparkContext, PolygonRDDInputLocation, FileDataSplitter.GEOJSON, false)
  val objectRDD = new PointRDD(spark.sparkContext, PointRDDInputLocation, PointRDDOffset, PointRDDSplitter, false)
  objectRDD.analyze()
  objectRDD.spatialPartitioning(joinQueryPartitioningType)
  queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

  queryWindowRDD.buildIndex(PolygonRDDIndexType, true)

  val resultSize = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, true, false)
  println("s")
  val geometries = resultSize.rdd.map[String](x => x._1.toText)
//  geometries.foreach(println)
  println("s")
//  resultSize.rdd.map[Int](x => x._2.size).foreach(println)

  resultSize.rdd.map[Array[String]](x => x._2.toArray.map(x => x.asInstanceOf[Point].toText)).foreach(x => println(x.mkString(" :: ")))
//  objectRDD.getRawSpatialRDD.rdd.map(x => x.asInstanceOf[Geometry].getUserData.toString.getBytes).foreach(println)
//  val splitted = resultSize.rdd.map[Array[Array[Byte]]](f => {
//    val seq1 = f._1
//    val seq2 = f._2
//
////    seq2.toArray.map(geom => GeometrySerializer.serialize(seq2))
//  })print(GeometryFactory.geometry_from_bytes(BinaryParser(bin_parser.bytes[293:])))

  println("s")
  //  GeoSerializer.testSer(spark)
//  println(GeoSerializer.serialize(spatialRDD.getRawSpatialRDD).rdd.map(x => GeometrySerializer.deserialize(ArrayData.toArrayData(x))).foreach(println))
}
