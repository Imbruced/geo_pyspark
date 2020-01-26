    # GeoSpark Core

The page outlines the steps to create Spatial RDDs and run spatial queries using GeoSpark-core. ==The example code is written in Scala but also works for Java==.


## Set up dependencies


## Initiate SparkContext

```python
from pyspark import SparkConf, SparkContext

from geo_pyspark.utils import KryoSerializer, GeoSparkKryoRegistrator

conf = SparkConf()
conf.setAppName("GeoSparkRunnableExample")
conf.setMaster("local[*]")
conf.set("spark.serializer", KryoSerializer.getName)
conf.set("spark.kryo.registrator", GeoSparkKryoRegistrator.getName)
sc = SparkContext(conf)
```

!!!warning
    GeoSpark has a suite of well-written geometry and index serializers. Forgetting to enable these serializers will lead to high memory consumption.

If you add ==the GeoSpark full dependencies== as suggested above, please use the following two lines to enable GeoSpark Kryo serializer instead:
```python
conf.set("spark.serializer", KryoSerializer.getName)
conf.set("spark.kryo.registrator", GeoSparkKryoRegistrator.getName)
```

## Create a SpatialRDD

### Create a typed SpatialRDD
GeoSpark-core provides three special SpatialRDDs:
<li> PointRDD </li>
<li> PolygonRDD </li> 
<li> LineStringRDD </li>
<li> CircleRDD </li>
<li> RectangleRDD </li>
<br>

They can be loaded from CSV, TSV, WKT, WKB, Shapefiles, GeoJSON formats.
To pass the format to SpatialRDD constructor please use <b> FileDataSplitter </b> enumeration. 

geo_pyspark SpatialRDDs (and other classes when it was necessary) have implemented meta classes which allow 
to use overloaded functions how Scala/Java GeoSpark API allows. ex. 


```python
from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import FileDataSplitter

input_location = "checkin.csv"
offset = 0  # The point long/lat starts from Column 0
splitter = FileDataSplitter.CSV # FileDataSplitter enumeration
carry_other_attributes = True  # Carry Column 2 (hotel, gas, bar...)
level = StorageLevel.MEMORY_ONLY # Storage level from pyspark
s_epsg = "epsg:4326" # Source epsg code
t_epsg = "epsg:5070" # target epsg code

point_rdd = PointRDD(sc, input_location, offset, splitter, carry_other_attributes)

point_rdd = PointRDD(sc, input_location, splitter, carry_other_attributes, level, s_epsg, t_epsg)

point_rdd = PointRDD(
    sparkContext=sc,
    InputLocation=input_location,
    Offset=offset,
    splitter=splitter,
    carryInputData=carry_other_attributes
)
```


#### From SparkSQL DataFrame
To create spatialRDD from other formats you can use adapter between Spark DataFrame and SpatialRDD
1. Load data in GeoSparkSQL.

```python
csv_point_input_location= "/tests/resources/county_small.tsv"

df = spark.read.\
    format("csv").\
    option("delimiter", "\t").\
    option("header", "false").\
    load(csv_point_input_location)

df.createOrReplaceTempView("counties")

```

2. Create a Geometry type column in GeoSparkSQL
```python
spatial_df = spark.sql(
    """
        SELECT ST_GeomFromWKT(_c0) as geom, _c6 as county_name
        FROM inputtable
    """
)
spatial_df.printSchema()
```

```
root
 |-- geom: geometry (nullable = false)
 |-- county_name: string (nullable = true)
```

3. Use GeoSparkSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD
```python
from geo_pyspark.utils.adapter import Adapter

spatial_rdd = Adapter.toSpatialRdd(spatial_df)
spatial_rdd.analyze()

spatial_rdd.boundaryEnvelope

<geo_pyspark.core.geom_types.Envelope object at 0x7f1e5f29fe10>
```

For WKT/WKB/GeoJSON data, please use ==ST_GeomFromWKT / ST_GeomFromWKB / ST_GeomFromGeoJSON== instead.
    
## Transform the Coordinate Reference System

## Read other attributes in an SpatialRDD

Each SpatialRDD can carry non-spatial attributes such as price, age and name as long as the user sets ==carryOtherAttributes== as [TRUE](#create-a-spatialrdd).

The other attributes are combined together to a string and stored in ==UserData== field of each geometry.

To retrieve the UserData field, use the following code:
```python
rdd_with_other_attributes = object_rdd.rawSpatialRDD.map(lambda x: x.getUserData())
``` 

## Write a Spatial Range Query

```python
from geo_pyspark.core import Envelope
from geo_pyspark.core.spatialOperator import RangeQuery
# TODO Add other geometries to range query window

range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
consider_boundary_intersection = False  ## Only return gemeotries fully covered by the window
using_index = False
query_result = RangeQuery.SpatialRangeQuery(spatial_rdd, range_query_window, consider_boundary_intersection, using_index)
```

!!!note
    Spatial range query is equal to ==ST_Within== and ==ST_Intersects== in Spatial SQL. An example query is as follows:
    ```SQL
    SELECT *
    FROM checkin
    WHERE ST_Intersects(queryWindow, checkin.location)
    ```

### Range query window

Besides the rectangle (Envelope) type range query window, GeoSpark range query window can be 
<li> Point </li> 
<li> Polygon </li>
<li> LineString </li>
</br>

The code to create a point is as follows:
To create shapely geometries please follow official shapely <a href=""> documentation </a>  



### Use spatial indexes

GeoSpark provides two types of spatial indexes,
<li> Quad-Tree </li>
<li> R-Tree </li>
Once you specify an index type, 
GeoSpark will build a local tree index on each of the SpatialRDD partition.

To utilize a spatial index in a spatial range query, use the following code:

```python
range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
consider_boundary_intersection = False ## Only return gemeotries fully covered by the window

build_on_spatial_partitioned_rdd = False ## Set to TRUE only if run join query
spatial_rdd.buildIndex(IndexType.QUADTREE, build_on_spatial_partitioned_rdd)

using_index = True

query_result = RangeQuery.SpatialRangeQuery(
    spatial_rdd,
    range_query_window,
    consider_boundary_intersection,
    using_index
)
```

!!!tip
    Using an index might not be the best choice all the time because building index also takes time. A spatial index is very useful when your data is complex polygons and line strings.

### Output format

The output format of the spatial range query is another SpatialRDD.

## Write a Spatial KNN Query

A spatial K Nearnest Neighbor query takes as input a K, a query point and an SpatialRDD and finds the K geometries in the RDD which are the closest to he query point.

Assume you now have an SpatialRDD (typed or generic). You can use the following code to issue an Spatial KNN Query on it.

```python
point = Point(-84.01, 34.01)
k = 1000 ## K Nearest Neighbors
using_index = False
result = KNNQuery.SpatialKnnQuery(object_rdd, point, k, using_index)
```

!!!note
    Spatial KNN query that returns 5 Nearest Neighbors is equal to the following statement in Spatial SQL
    ```SQL
    SELECT ck.name, ck.rating, ST_Distance(ck.location, myLocation) AS distance
    FROM checkins ck
    ORDER BY distance DESC
    LIMIT 5
    ```

### Query center geometry

Besides the Point type, GeoSpark KNN query center can be 
<li> Polygon </li>
<li> LineString </li>

To create Polygon or Linestring object please follow Shapely official <a href="https://shapely.readthedocs.io/en/stable/manual.html"> documentation </a>

### Use spatial indexes

To utilize a spatial index in a spatial KNN query, use the following code:

```python
point = Point(-84.01, 34.01)
k = 1000 ## K Nearest Neighbors


build_on_spatial_partitioned_rdd = False ## Set to TRUE only if run join query
spatial_rdd.buildIndex(IndexType.RTREE, build_on_spatial_partitioned_rdd)

using_index = True
result = KNNQuery.SpatialKnnQuery(spatial_rdd, point, k, using_index)
```

!!!warning
    Only R-Tree index supports Spatial KNN query

### Output format

The output format of the spatial KNN query is a list of geometries. 
The list has K geometry objects.

## Write a Spatial Join Query

A spatial join query takes as input two Spatial RDD A and B. For each geometry in A, finds the geometries (from B) covered/intersected by it. A and B can be any geometry type and are not necessary to have the same geometry type.

Assume you now have two SpatialRDDs (typed or generic). You can use the following code to issue an Spatial Join Query on them.

```Scala
val considerBoundaryIntersection = false // Only return gemeotries fully covered by each query window in queryWindowRDD
val usingIndex = false

objectRDD.analyze()

objectRDD.spatialPartitioning(GridType.KDBTREE)
queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

val result = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, usingIndex, considerBoundaryIntersection)
```

!!!note
    Spatial join query is equal to the following query in Spatial SQL:
    ```SQL
    SELECT superhero.name
    FROM city, superhero
    WHERE ST_Contains(city.geom, superhero.geom);
    ```
    Find the super heros in each city

### Use spatial partitioning

GeoSpark spatial partitioning method can significantly speed up the join query. Three spatial partitioning methods are available: KDB-Tree, Quad-Tree and R-Tree. Two SpatialRDD must be partitioned by the same way.

If you first partition SpatialRDD A, then you must use the partitioner of A to partition B.

```Scala
objectRDD.spatialPartitioning(GridType.KDBTREE)
queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
```

Or 

```Scala
queryWindowRDD.spatialPartitioning(GridType.KDBTREE)
objectRDD.spatialPartitioning(queryWindowRDD.getPartitioner)
```


### Use spatial indexes

To utilize a spatial index in a spatial join query, use the following code:

```Scala
objectRDD.spatialPartitioning(joinQueryPartitioningType)
queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
val usingIndex = true
queryWindowRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

val result = JoinQuery.SpatialJoinQueryFlat(objectRDD, queryWindowRDD, usingIndex, considerBoundaryIntersection)
```

The index should be built on either one of two SpatialRDDs. In general, you should build it on the larger SpatialRDD.

### Output format

The output format of the spatial join query is a PairRDD. In this PairRDD, each object is a pair of two geometries. The left one is the geometry from objectRDD and the right one is the geometry from the queryWindowRDD.

```
Point,Polygon
Point,Polygon
Point,Polygon
Polygon,Polygon
LineString,LineString
Polygon,LineString
...
```

Each object on the left is covered/intersected by the object on the right.

## Write a Distance Join Query

A distance join query takes two spatial RDD assuming that we have two SpatialRDD's:
<li> object_rdd </li>
<li> spatial_rdd </li>

And finds the geometries (from spatial_rdd) are within given distance to it. spatial_rdd and object_rdd
can be any geometry type (point, line, polygon) and are not necessary to have the same geometry type
 
You can use the following code to issue an Distance Join Query on them.

```python
object_rdd.analyze()

circle_rdd = CircleRDD(object_rdd, 0.1) ## Create a CircleRDD using the given distance
circle_rdd.analyze()

circle_rdd.spatialPartitioning(GridType.KDBTREE)
spatial_rdd.spatialPartitioning(circle_rdd.getPartitioner())

consider_boundary_intersection = False ## Only return gemeotries fully covered by each query window in queryWindowRDD
using_index = False

result = JoinQuery.DistanceJoinQueryFlat(spatial_rdd, circle_rdd, using_index, consider_boundary_intersection)
```

Result for this query is RDD which holds two GeoData objects within list of lists.
Example:
```python
result.collect()
[[GeoData, GeoData], [GeoData, GeoData] ...]
```

    
## Save to permanent storage

You can always save an SpatialRDD back to some permanent storage such as HDFS and Amazon S3. You can save distributed SpatialRDD to WKT, GeoJSON and object files.

!!!note
    Non-spatial attributes such as price, age and name will also be stored to permanent storage.

### Save an SpatialRDD (not indexed)

Typed SpatialRDD and generic SpatialRDD can be saved to permanent storage.

#### Save to distributed WKT text file

Use the following code to save an SpatialRDD as a distributed WKT text file:

```Scala
objectRDD.rawSpatialRDD.saveAsTextFile("hdfs://PATH")
objectRDD.rawSpatialRDD.saveAsWKT("hdfs://PATH")
```

#### Save to distributed WKB text file

Use the following code to save an SpatialRDD as a distributed WKB text file:

```Scala
objectRDD.saveAsWKB("hdfs://PATH")
```

#### Save to distributed GeoJSON text file

Use the following code to save an SpatialRDD as a distributed GeoJSON text file:

```Scala
objectRDD.saveAsGeoJSON("hdfs://PATH")
```


#### Save to distributed object file

Use the following code to save an SpatialRDD as a distributed object file:

```python
object_rdd.rawJvmSpatialRDD.saveAsObjectFile("hdfs://PATH")
```

!!!note
    Each object in a distributed object file is a byte array (not human-readable). This byte array is the serialized format of a Geometry or a SpatialIndex.

### Save an SpatialRDD (indexed)

Indexed typed SpatialRDD and generic SpatialRDD can be saved to permanent storage. However, the indexed SpatialRDD has to be stored as a distributed object file.

#### Save to distributed object file

Use the following code to save an SpatialRDD as a distributed object file:

```python
objectRDD.indexedRawRDD.saveAsObjectFile("hdfs://PATH")
```

### Save an SpatialRDD (spatialPartitioned W/O indexed)

A spatial partitioned RDD can be saved to permanent storage but Spark is not able to maintain the same RDD partition Id of the original RDD. This will lead to wrong join query results. We are working on some solutions. Stay tuned!

### Reload a saved SpatialRDD

You can easily reload an SpatialRDD that has been saved to ==a distributed object file==.

#### Load to a typed SpatialRDD

Use the following code to reload the PointRDD/PolygonRDD/LineStringRDD:

```python
var savedRDD = new PointRDD(sc.objectFile[Point]("hdfs://PATH"))

var savedRDD = new PointRDD(sc.objectFile[Polygon]("hdfs://PATH"))

var savedRDD = new PointRDD(sc.objectFile[LineString]("hdfs://PATH"))
```

#### Load to a generic SpatialRDD

Use the following code to reload the SpatialRDD:

```Scala
var savedRDD = new SpatialRDD[Geometry]
savedRDD.rawSpatialRDD = sc.objectFile[Geometry]("hdfs://PATH")
```

Use the following code to reload the indexed SpatialRDD:
```Scala
var savedRDD = new SpatialRDD[Geometry]
savedRDD.indexedRawRDD = sc.objectFile[SpatialIndex]("hdfs://PATH")
```
