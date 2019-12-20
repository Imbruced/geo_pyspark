package org.imbruced.geo_pyspark.serializers

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}

object GeoSparkSpatialRDDToPython {
  def serialize(spatialRDD: JavaRDD[Geometry], serializer: SpatialRDDSerializer): JavaRDD[Array[Byte]] ={
    serializer.serializeToPython(spatialRDD)
  }
  def serialize(spatialRDD: JavaPairRDD[Geometry, Geometry], serializer: SpatialRDDSerializer): JavaRDD[Array[Byte]] = {
    serializer.serializeToPython(spatialRDD)
  }

  def serializeWithRightHashSet(spatialRDD: JavaPairRDD[Geometry, java.util.HashSet[Geometry]], serializer: SpatialRDDSerializer): JavaRDD[Array[Byte]] = {
    serializer.serializeToPythonHashSet(spatialRDD)
  }
}
