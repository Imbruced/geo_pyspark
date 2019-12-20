package org.imbruced.geo_pyspark.serializers

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.datasyslab.geosparksql.utils.GeometrySerializer

object GeoSerializerNoUserAttributes extends SpatialRDDSerializer{
  def serializeToPython(spatialRDD: JavaRDD[Geometry]): JavaRDD[Array[Byte]] = {
     spatialRDD.rdd.map[Array[Byte]](geom => Array(0.toByte) ++GeometrySerializer.serialize(geom)).toJavaRDD()
  }

  def serializeToPythonHashSet(spatialRDD: JavaPairRDD[Geometry, java.util.HashSet[Geometry]]): JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](
      pairRDD => {
        val rightGeometry = pairRDD._2
        val leftGeometry = pairRDD._1
        Array(1.toByte) ++ GeometrySerializer.serialize(leftGeometry.asInstanceOf[Geometry]) ++
        Array(1.toByte) ++ Array(pairRDD._2.toArray.length.toByte) ++
        rightGeometry.toArray.flatMap(geom => GeometrySerializer.serialize(geom.asInstanceOf[Geometry]) ++ Array(10.toFloat.toByte))
      }
    )
  }

  def serializeToPython(spatialRDD: JavaPairRDD[Geometry, Geometry]): JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](pairRDD =>
      Array(1.toByte) ++ GeometrySerializer.serialize(pairRDD._1) ++ Array(0.toByte) ++ GeometrySerializer.serialize(pairRDD._2)
    ).toJavaRDD()
  }

}
