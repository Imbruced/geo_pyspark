package org.imbruced.geo_pyspark.serializers

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.datasyslab.geosparksql.utils.GeometrySerializer

object GeoSerializerNoUserAttributes extends SpatialRDDSerializer{
  def serializeToPython(spatialRDD: JavaRDD[Geometry]): JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](geom => GeometrySerializer.serialize(geom)).toJavaRDD()
  }

  def serializeToPythonHashSet(spatialRDD: JavaPairRDD[Geometry, java.util.HashSet[Geometry]]): JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](
      pairRDD => {
        val rightGeometry = pairRDD._2
        val leftGeometry = pairRDD._1
        GeometrySerializer.serialize(leftGeometry.asInstanceOf[Geometry]) ++
        Array(10.toFloat.toByte) ++ Array(pairRDD._2.size.toByte) ++ Array(10.toFloat.toByte) ++
        rightGeometry.toArray.flatMap(geom => GeometrySerializer.serialize(geom.asInstanceOf[Geometry]) ++ Array(10.toFloat.toByte))
      }
    )
  }

  def serializeToPython(spatialRDD: JavaPairRDD[Geometry, Geometry]): JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](pairRDD =>
      GeometrySerializer.serialize(pairRDD._1) ++ Array(10.toFloat.toByte) ++ GeometrySerializer.serialize(pairRDD._2)
    ).toJavaRDD()
  }

}
