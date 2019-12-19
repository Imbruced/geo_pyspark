package org.imbruced.geo_pyspark.serializers

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}

abstract class SpatialRDDSerializer {
  def serializeToPython(spatialRDD: JavaRDD[Geometry]): JavaRDD[Array[Byte]]
  def serializeToPythonHashSet(spatialRDD: JavaPairRDD[Geometry, java.util.HashSet[Geometry]]): JavaRDD[Array[Byte]]
  def serializeToPython(spatialRDD: JavaPairRDD[Geometry, Geometry]): JavaRDD[Array[Byte]]
}
