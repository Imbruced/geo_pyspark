package org.imbruced.geo_pyspark.serializers

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.datasyslab.geosparksql.utils.GeometrySerializer


object GeoSerializerData {
  def serializeToPython(spatialRDD: JavaRDD[Geometry]): JavaRDD[Array[Byte]] = {

    spatialRDD.rdd.map[Array[Byte]](geom =>{
      val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      sizeBuffer.putInt(0)

      serializeGeomToPython(geom) ++ sizeBuffer.array()
    }


    ).toJavaRDD()
  }

  def serializeGeomToPython(geom: Geometry): Array[Byte] = {
      val userData = geom.getUserData
      geom.setUserData("")
      val serializedGeom = GeometrySerializer.serialize(geom)
      val userDataBinary = userData.asInstanceOf[String].getBytes(StandardCharsets.UTF_8)
      val userDataLengthArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      userDataLengthArray.putInt(userDataBinary.length)

      userDataLengthArray.array() ++ serializedGeom ++ userDataBinary

  }

  def serializeToPythonHashSet(spatialRDD: JavaPairRDD[Geometry, java.util.HashSet[Geometry]]): JavaRDD[Array[Byte]] = {

    spatialRDD.rdd.map[Array[Byte]](
      pairRDD => {

        val rightGeometry = pairRDD._2
        val leftGeometry = pairRDD._1
        val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        sizeBuffer.putInt(rightGeometry.toArray.length)

        serializeGeomToPython(leftGeometry) ++
          sizeBuffer.array() ++
          rightGeometry.toArray().flatMap(geometry => serializeGeomToPython(geometry.asInstanceOf[Geometry]))
      }
    )
  }

  def serializeToPython(spatialRDD: JavaPairRDD[Geometry, Geometry]): JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](pairRDD =>{
      val leftGeometry = pairRDD._1
      val rightGeometry = pairRDD._2
      val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      sizeBuffer.putInt(1)
      serializeGeomToPython(leftGeometry) ++ sizeBuffer.array() ++ serializeGeomToPython(rightGeometry)
    }
    ).toJavaRDD()
  }

  def deserializeUserData(userData: Array[java.lang.Byte]): String = {
    val in = new ByteArrayInputStream(userData.map(x=> x.toByte))
    val kryo2 = new Kryo()
    val input = new Input(in)
    kryo2.readObject(input, "".getClass)
  }
}
