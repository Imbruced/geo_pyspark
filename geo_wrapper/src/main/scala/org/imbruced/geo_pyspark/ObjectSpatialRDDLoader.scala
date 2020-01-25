package org.imbruced.geo_pyspark

import com.vividsolutions.jts.geom.{Geometry, LineString, Point, Polygon}
import com.vividsolutions.jts.index.SpatialIndex
import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.datasyslab.geospark.spatialRDD.{LineStringRDD, PointRDD, PolygonRDD, SpatialRDD}

object ObjectSpatialRDDLoader {
  def loadPointSpatialRDD(sc:SparkContext, path: String, geomType: String): PointRDD = {
    new PointRDD(sc.objectFile[Point](path))
  }

  def loadPolygonSpatialRDD(sc: SparkContext, path: String): PolygonRDD = {
    new PolygonRDD(sc.objectFile[Polygon](path))
  }

  def loadSpatialRDD(sc: SparkContext, path: String): SpatialRDD[Geometry] = {
    val spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = sc.objectFile[Geometry](path)
    spatialRDD
  }

  def loadLineStringSpatialRDD(sc: SparkContext, path: String): LineStringRDD = {
    new LineStringRDD(sc.objectFile[LineString](path))
  }

  def loadIndexRDD(sc: SparkContext, path: String): JavaRDD[SpatialIndex] = {
    sc.objectFile[SpatialIndex](path)
  }
}
