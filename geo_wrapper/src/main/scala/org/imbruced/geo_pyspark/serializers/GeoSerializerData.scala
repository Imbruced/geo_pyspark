package org.imbruced.geo_pyspark.serializers

import java.nio.{ByteBuffer, ByteOrder}

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.datasyslab.geosparksql.utils.GeometrySerializer

import scala.annotation.tailrec

object GeoSerializerData {
  def serializeToPython(spatialRDD: JavaRDD[Geometry]): JavaRDD[Array[Byte]] = {

    spatialRDD.rdd.map[Array[Byte]](geom =>{
      val userDataLengthArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      val serializedGeom = GeometrySerializer.serialize(geom)

      userDataLengthArray.putInt(serializedGeom.length+4)

      sizeBuffer.putInt(0)

      userDataLengthArray.array() ++ serializedGeom ++ sizeBuffer.array()
    }


    ).toJavaRDD()
  }

  def serializeMultipleGeom(originIndex: Int, geometries: Array[Geometry]): Array[Byte] = {

    @tailrec
    def accumulate(startingIndex: Int, accumByteArray: Array[Byte], indexData: Int): Array[Byte] = {
      if (geometries.length <= indexData) accumByteArray
      else{
        val userDataLengthArrayRight = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        val serializedGeom =  GeometrySerializer.serialize(geometries(indexData))
        val userDataLength = serializedGeom.length + startingIndex + 4
        userDataLengthArrayRight.putInt(userDataLength)
        println(userDataLength)
        accumulate(userDataLength, accumByteArray ++ userDataLengthArrayRight.array() ++ serializedGeom, indexData+1)
      }

    }

    accumulate(originIndex, Array(), 0)

  }

  def serializeToPythonHashSet(spatialRDD: JavaPairRDD[Geometry, java.util.HashSet[Geometry]]): JavaRDD[Array[Byte]] = {

    spatialRDD.rdd.map[Array[Byte]](
      pairRDD => {
        val rightGeometry = pairRDD._2
        val leftGeometry = pairRDD._1
        val sizeBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        val userDataLengthArray = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        val serializedGeom = GeometrySerializer.serialize(leftGeometry.asInstanceOf[Geometry])
        val userDataLength = serializedGeom.length+4

        userDataLengthArray.putInt(userDataLength)

        sizeBuffer.putInt(rightGeometry.toArray.length)
        userDataLengthArray.array() ++ serializedGeom ++
          sizeBuffer.array() ++
          serializeMultipleGeom(userDataLength+4, rightGeometry.toArray().map(geometry => geometry.asInstanceOf[Geometry]))
      }
    )
  }

  def serializeToPython(spatialRDD: JavaPairRDD[Geometry, Geometry]): JavaRDD[Array[Byte]] = {
    spatialRDD.rdd.map[Array[Byte]](pairRDD =>
      Array(1.toByte) ++ GeometrySerializer.serialize(pairRDD._1) ++ Array(0.toByte) ++ GeometrySerializer.serialize(pairRDD._2)
    ).toJavaRDD()
  }
}
