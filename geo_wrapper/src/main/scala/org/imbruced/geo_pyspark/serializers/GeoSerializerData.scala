package org.imbruced.geo_pyspark.serializers

import java.nio.{ByteBuffer, ByteOrder}

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.datasyslab.geosparksql.utils.GeometrySerializer

object GeoSerializerData {
  def serializeToPython(spatialRDD: JavaRDD[Geometry]): JavaRDD[Array[Byte]] = {

    spatialRDD.rdd.map[Array[Byte]](geom =>{
      val userDataLength: Int = geom.getUserData.asInstanceOf[String].length
      val userDataLengthArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)

      userDataLengthArray.putInt(userDataLength)

      sizeBuffer.putInt(0)

      userDataLengthArray.array() ++ GeometrySerializer.serialize(geom) ++ sizeBuffer.array()
    }


    ).toJavaRDD()
  }

  private def serializeMultipleGeom(geom: Geometry): Array[Byte] = {
    val userDataLengthArrayRight = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
    userDataLengthArrayRight.putInt(geom.getUserData.toString.length)
    userDataLengthArrayRight.array() ++ GeometrySerializer.serialize(geom)
  }
  def serializeToPythonHashSet(spatialRDD: JavaPairRDD[Geometry, java.util.HashSet[Geometry]]): JavaRDD[Array[Byte]] = {

    spatialRDD.rdd.map[Array[Byte]](
      pairRDD => {
        val rightGeometry = pairRDD._2
        val leftGeometry = pairRDD._1
        val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        val userDataLength: Int = leftGeometry.asInstanceOf[Geometry].getUserData.asInstanceOf[String].length
        val userDataLengthArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)

        userDataLengthArray.putInt(userDataLength)

        sizeBuffer.putInt(rightGeometry.toArray.length)
        userDataLengthArray.array() ++ GeometrySerializer.serialize(leftGeometry.asInstanceOf[Geometry]) ++
          sizeBuffer.array() ++
          rightGeometry.toArray.flatMap(geom =>serializeMultipleGeom(geom.asInstanceOf[Geometry])
          )
      }
    )
  }

  def serializeToPython(spatialRDD: JavaPairRDD[Geometry, Geometry]): JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](pairRDD =>
      Array(1.toByte) ++ GeometrySerializer.serialize(pairRDD._1) ++ Array(0.toByte) ++ GeometrySerializer.serialize(pairRDD._2)
    ).toJavaRDD()
  }
}
