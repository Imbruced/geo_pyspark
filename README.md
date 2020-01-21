# Welcome to geo_pyspark documentation!


# Introduction

Package is a Python wrapper on scala library GeoSparkSQL. Official repository for GeoSpark can be found at https://github.com/DataSystemsLab/GeoSpark.

Package allow to use all GeoSparkSQL functions and transform it to Python Shapely geometry objects. Also it allows to create Spark DataFrame with GeoSpark UDT from Shapely geometry objects. Spark DataFrame can be converted to GeoPandas easily, in addition all fiona drivers for shape file are available to load data from files and convert them to Spark DataFrame. Please look at examples.



# Installation


geo_pyspark depnds on Python packages and Scala libraries. To see all dependencies
please look at Dependencies section.
https://pypi.org/project/pyspark/.

Package needs 3 jar files to work properly:

- geospark-sql_2.2-1.2.0.jar
- geospark-1.2.0.jar
- geo_wrapper.jar

Where 2.2 is a Spark version and 1.2.0 is GeoSpark version. Jar files are placed in geo_pyspark/jars. For newest GeoSpark release jar files are places in subdirectories named as Spark version. Example, jar files for SPARK 2.4 can be found in directory geo_pyspark/jars/2_4.

For older version please find appropriate jar files in directory geo_pyspark/jars/previous.

It is possible to automatically add jar files for newest GeoSpark version. Please use code as follows:


```python

  from pyspark.sql import SparkSession

  from geo_pyspark.register import upload_jars
  from geo_pyspark.register import GeoSparkRegistrator

  upload_jars()

  spark = SparkSession.builder.\
        getOrCreate()

  GeoSparkRegistrator.registerAll(spark)

```

Function

```python

  upload_jars()


```

uses findspark Python package to upload jar files to executor and nodes. To avoid copying all the time, jar files can be put in directory SPARK_HOME/jars or any other path specified in Spark config files.



## Installing from wheel file


```bash

pipenv run python -m pip install dist/geo_pyspark-0.2.0-py3-none-any.whl

```

or

```bash

  pip install dist/geo_pyspark-0.2.0-py3-none-any.whl


```

## Installing from source


```bash

  python3 setup.py install

```

# Core Classes and methods.


`GeoSparkRegistrator.registerAll(spark: pyspark.sql.SparkSession) -> bool`

This is the core of whole package. Class method registers all GeoSparkSQL functions (available for used GeoSparkSQL version).
To check available functions please look at GeoSparkSQL section.
:param spark: pyspark.sql.SparkSession, spark session instance


`upload_jars() -> NoReturn`

Function uses `findspark` Python module to upload newest GeoSpark jars to Spark executor and nodes.

`GeometryType()`

Class which handle serialization and deserialization between GeoSpark geometries and Shapely BaseGeometry types.

`KryoSerializer.getName -> str`
Class property which returns org.apache.spark.serializer.KryoSerializer string, which simplify using GeoSpark Serializers.

`GeoSparkKryoRegistrator.getName -> str`
Class property which returns org.datasyslab.geospark.serde.GeoSparkKryoRegistrator string, which simplify using GeoSpark Serializers.



# Writing Application

Use KryoSerializer.getName and GeoSparkKryoRegistrator.getName class properties to reduce memory impact, reffering to  <a href="https://datasystemslab.github.io/GeoSpark/tutorial/sql/"> GeoSpark docs </a>. To do that use spark config as follows:

```python

.config("spark.serializer", KryoSerializer.getName)
.config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName)

```

If jars was not uploaded manually please use function `upload_jars()`

To turn on GeoSparkSQL function inside pyspark code use GeoSparkRegistrator.registerAll method on existing pyspark.sql.SparkSession instance ex.

`GeoSparkRegistrator.registerAll(spark)`

After that all the functions from GeoSparkSQL will be available, moreover using collect or toPandas methods on Spark DataFrame will return Shapely BaseGeometry objects. Based on GeoPandas DataFrame, Pandas DataFrame with shapely objects or Sequence with shapely objects, Spark DataFrame can be created using spark.createDataFrame method. To specify Schema with geometry inside please use `GeometryType()` instance (look at examples section to see that in practice).



# Examples

## GeoSparkSQL


All GeoSparkSQL functions (list depends on GeoSparkSQL version) are available in Python API. For documentation please look at <a href="https://datasystemslab.github.io/GeoSpark/api/sql/GeoSparkSQL-Overview/"> GeoSpark website</a>

For example use GeoSparkSQL for Spatial Join.

```python3

counties = spark.\
    read.\
    option("delimiter", "|").\
    option("header", "true").\
    csv("counties.csv")

counties.createOrReplaceTempView("county")

counties_geom = spark.sql(
      "SELECT county_code, st_geomFromWKT(geom) as geometry from county"
)

points = gpd.read_file("gis_osm_pois_free_1.shp")

points_geom = spark.createDataFrame(
    points[["fclass", "geometry"]]
)

counties_geom.show(5, False)

```
```
+-----------+--------------------+
|county_code|            geometry|
+-----------+--------------------+
|       1815|POLYGON ((21.6942...|
|       1410|POLYGON ((22.7238...|
|       1418|POLYGON ((21.1100...|
|       1425|POLYGON ((20.9891...|
|       1427|POLYGON ((19.5087...|
+-----------+--------------------+
```
```python3
points_geom.show(5, False)
```
```
+---------+-----------------------------+
|fclass   |geometry                     |
+---------+-----------------------------+
|camp_site|POINT (15.3393145 52.3504247)|
|chalet   |POINT (14.8709625 52.691693) |
|motel    |POINT (15.0946636 52.3130396)|
|atm      |POINT (15.0732014 52.3141083)|
|hotel    |POINT (15.0696777 52.3143013)|
+---------+-----------------------------+
```

```python3

points_geom.createOrReplaceTempView("pois")
counties_geom.createOrReplaceTempView("counties")

spatial_join_result = spark.sql(
    """
        SELECT c.county_code, p.fclass
        FROM pois AS p, counties AS c
        WHERE ST_Intersects(p.geometry, c.geometry)
    """
)

spatial_join_result.explain()

```
```
== Physical Plan ==
*(2) Project [county_code#230, fclass#239]
+- RangeJoin geometry#240: geometry, geometry#236: geometry, true
   :- Scan ExistingRDD[fclass#239,geometry#240]
   +- Project [county_code#230, st_geomfromwkt(geom#232) AS geometry#236]
      +- *(1) FileScan csv [county_code#230,geom#232] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/home/pkocinski001/Desktop/projects/geo_pyspark_installed/counties.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<county_code:string,geom:string>
```
Calculating Number of Pois within counties per fclass.

```python3
pois_per_county = spatial_join_result.groupBy("county_code", "fclass"). \
    count()

pois_per_county.show(5, False)

```
```
+-----------+---------+-----+
|county_code|fclass   |count|
+-----------+---------+-----+
|0805       |atm      |6    |
|0805       |bench    |75   |
|0803       |museum   |9    |
|0802       |fast_food|5    |
|0862       |atm      |20   |
+-----------+---------+-----+
```

## Integration with GeoPandas and Shapely


geo_pyspark has implemented serializers and deserializers which allows to convert GeoSpark Geometry objects into Shapely BaseGeometry objects. Based on that it is possible to load the data with geopandas from file (look at Fiona possible drivers) and create Spark DataFrame based on GeoDataFrame object.

Example, loading the data from shapefile using geopandas read_file method and create Spark DataFrame based on GeoDataFrame:

```python

  import geopandas as gpd
  from pyspark.sql import SparkSession

  from geo_pyspark.register import GeoSparkRegistrator

  spark = SparkSession.builder.\
        getOrCreate()

  GeoSparkRegistrator.registerAll(spark)

  gdf = gpd.read_file("gis_osm_pois_free_1.shp")

  spark.createDataFrame(
    gdf
  ).show()

```

```

      +---------+----+-----------+--------------------+--------------------+
      |   osm_id|code|     fclass|                name|            geometry|
      +---------+----+-----------+--------------------+--------------------+
      | 26860257|2422|  camp_site|            de Kroon|POINT (15.3393145...|
      | 26860294|2406|     chalet|      Leśne Ustronie|POINT (14.8709625...|
      | 29947493|2402|      motel|                null|POINT (15.0946636...|
      | 29947498|2602|        atm|                null|POINT (15.0732014...|
      | 29947499|2401|      hotel|                null|POINT (15.0696777...|
      | 29947505|2401|      hotel|                null|POINT (15.0155749...|
      +---------+----+-----------+--------------------+--------------------+

```

Reading data with Spark and converting to GeoPandas

```python

    import geopandas as gpd
    from pyspark.sql import SparkSession

    from geo_pyspark.register import GeoSparkRegistrator

    spark = SparkSession.builder.\
        getOrCreate()

    GeoSparkRegistrator.registerAll(spark)

    counties = spark.\
    read.\
    option("delimiter", "|").\
    option("header", "true").\
    csv("counties.csv")

    counties.createOrReplaceTempView("county")

    counties_geom = spark.sql(
          "SELECT *, st_geomFromWKT(geom) as geometry from county"
    )

    df = counties_geom.toPandas()
    gdf = gpd.GeoDataFrame(df, geometry="geometry")

    gdf.plot(
        figsize=(10, 8),
        column="value",
        legend=True,
        cmap='YlOrBr',
        scheme='quantiles',
        edgecolor='lightgray'
    )

```
<br>
<br>

![poland_image](https://user-images.githubusercontent.com/22958216/67603296-c08b4680-f778-11e9-8cde-d2e14ffbba3b.png)

<br>
<br>

## Creating Spark DataFrame based on shapely objects

### Supported Shapely objects

| shapely object  | Available          |
|-----------------|--------------------|
| Point           | :heavy_check_mark: |
| MultiPoint      | :heavy_check_mark: |
| LineString      | :heavy_check_mark: |
| MultiLinestring | :heavy_check_mark: |
| Polygon         | :heavy_check_mark: |
| MultiPolygon    | :heavy_check_mark: |

To create Spark DataFrame based on mentioned Geometry types, please use <b> GeometryType </b> from  <b> geo_pyspark.sql.types </b> module. Converting works for list or tuple with shapely objects.

Schema for target table with integer id and geometry type can be defined as follow:

```python

from pyspark.sql.types import IntegerType, StructField, StructType

from geo_pyspark.sql.types import GeometryType

schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("geom", GeometryType(), False)
    ]
)

```

Also Spark DataFrame with geometry type can be converted to list of shapely objects with <b> collect </b> method.

### Example usage for Shapely objects

#### Point

```python
from shapely.geometry import Point

data = [
    [1, Point(21.0, 52.0)],
    [1, Point(23.0, 42.0)],
    [1, Point(26.0, 32.0)]
]


gdf = spark.createDataFrame(
    data,
    schema
)

gdf.show()

```

```
+---+-------------+
| id|         geom|
+---+-------------+
|  1|POINT (21 52)|
|  1|POINT (23 42)|
|  1|POINT (26 32)|
+---+-------------+
```

```python
gdf.printSchema()
```

```
root
 |-- id: integer (nullable = false)
 |-- geom: geometry (nullable = false)
```

#### MultiPoint

```python3

data = [
    [1, MultiPoint([[19.511463, 51.765158], [19.446408, 51.779752]])]
]

gdf = spark.createDataFrame(
    data,
    schema
).show(1, False)

```

```

+---+---------------------------------------------------------+
|id |geom                                                     |
+---+---------------------------------------------------------+
|1  |MULTIPOINT ((19.511463 51.765158), (19.446408 51.779752))|
+---+---------------------------------------------------------+


```

#### LineString

```python3

from shapely.geometry import LineString

line = [(40, 40), (30, 30), (40, 20), (30, 10)]

data = [
    [1, LineString(line1)]
]

gdf = spark.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```

```

+---+--------------------------------+
|id |geom                            |
+---+--------------------------------+
|1  |LINESTRING (10 10, 20 20, 10 40)|
+---+--------------------------------+

````

#### MultiLineString

```python3

from shapely.geometry import MultiLineString

line1 = [(10, 10), (20, 20), (10, 40)]
line2 = [(40, 40), (30, 30), (40, 20), (30, 10)]

data = [
    [1, MultiLineString([line1, line2])]
]

gdf = spark.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```

```

+---+---------------------------------------------------------------------+
|id |geom                                                                 |
+---+---------------------------------------------------------------------+
|1  |MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))|
+---+---------------------------------------------------------------------+

```

#### Polygon

```python3

from shapely.geometry import Polygon

polygon = Polygon(
    [
         [19.51121, 51.76426],
         [19.51056, 51.76583],
         [19.51216, 51.76599],
         [19.51280, 51.76448],
         [19.51121, 51.76426]
    ]
)

data = [
    [1, polygon]
]

gdf = spark.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```


```

+---+--------------------------------------------------------------------------------------------------------+
|id |geom                                                                                                    |
+---+--------------------------------------------------------------------------------------------------------+
|1  |POLYGON ((19.51121 51.76426, 19.51056 51.76583, 19.51216 51.76599, 19.5128 51.76448, 19.51121 51.76426))|
+---+--------------------------------------------------------------------------------------------------------+

```

#### MultiPolygon

```python3

from shapely.geometry import MultiPolygon

exterior_p1 = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
interior_p1 = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]

exterior_p2 = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]

polygons = [
    Polygon(exterior_p1, [interior_p1]),
    Polygon(exterior_p2)
]

data = [
    [1, MultiPolygon(polygons)]
]

gdf = spark.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```

```

+---+----------------------------------------------------------------------------------------------------------+
|id |geom                                                                                                      |
+---+----------------------------------------------------------------------------------------------------------+
|1  |MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0), (1 1, 1.5 1, 1.5 1.5, 1 1.5, 1 1)), ((0 0, 0 1, 1 1, 1 0, 0 0)))|
+---+----------------------------------------------------------------------------------------------------------+

```

## Supported versions


### Apache Spark

Currently package supports spark versions

<li> 2.2 </li>
<li> 2.3 </li>
<li> 2.4 </li>


### GeoSpark

<li> 1.2.0 </li>
<li> 1.1.3 </li>

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
GeoSpark-core provides three special SpatialRDDs: ==PointRDD, PolygonRDD, and LineStringRDD==. They can be loaded from CSV, TSV, WKT, WKB, Shapefiles, GeoJSON and NetCDF/HDF format.

#### PointRDD from CSV/TSV
Suppose we have a `checkin.csv` CSV file at Path `/Download/checkin.csv` as follows:
```
-88.331492,32.324142,hotel
-88.175933,32.360763,gas
-88.388954,32.357073,bar
-88.221102,32.35078,restaurant
```
This file has three columns and corresponding ==offsets==(Column IDs) are 0, 1, 2.
Use the following code to create a PointRDD

```python
from geo_pyspark.core.SpatialRDD import PointRDD
from geo_pyspark.core.enums import FileDataSplitter

point_rdd_input_location = "checkin.csv"
point_rdd_offset = 0  # The point long/lat starts from Column 0
point_rdd_splitter = FileDataSplitter.CSV
carry_other_attributes = True  # Carry Column 2 (hotel, gas, bar...)
object_rdd = PointRDD(sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, carry_other_attributes)
```

If the data file is in TSV format, just simply use the following line to replace the old FileDataSplitter:
```python
point_rdd_splitter = FileDataSplitter.TSV
```

#### PolygonRDD/LineStringRDD from CSV/TSV
In general, polygon and line string data is stored in WKT, WKB, GeoJSON and Shapefile formats instead of CSV/TSV because the geometries in a file may have different lengths. However, if all polygons / line strings in your CSV/TSV possess the same length, you can create PolygonRDD and LineStringRDD from these files.

Suppose we have a `checkinshape.csv` CSV file at Path `/Download/checkinshape.csv ` as follows:
```
-88.331492,32.324142,-88.331492,32.324142,-88.331492,32.324142,-88.331492,32.324142,-88.331492,32.324142,hotel
-88.175933,32.360763,-88.175933,32.360763,-88.175933,32.360763,-88.175933,32.360763,-88.175933,32.360763,gas
-88.388954,32.357073,-88.388954,32.357073,-88.388954,32.357073,-88.388954,32.357073,-88.388954,32.357073,bar
-88.221102,32.35078,-88.221102,32.35078,-88.221102,32.35078,-88.221102,32.35078,-88.221102,32.35078,restaurant
```

This file has 11 columns and corresponding offsets (Column IDs) are 0 - 10. Column 0 - 9 are 5 coordinates (longitude/latitude pairs). In this file, all geometries have the same number of coordinates. The geometries can be polyons or line strings.

!!!warning
	For polygon data, the last coordinate must be the same as the first coordinate because a polygon is a closed linear ring.
	
Use the following code to create a PolygonRDD.
```python
polygon_rdd_input_location = "checkinshape.csv"
polygon_rdd_start_offset = 0  # The coordinates start from Column 0
polygon_rdd_end_offset = 9  # The coordinates end at Column 8
polygon_rdd_splitter = FileDataSplitter.CSV
carry_other_attributes = True  # Carry Column 10 (hotel, gas, bar...)
object_rdd = PolygonRDD(sc, polygon_rdd_input_location, polygon_rdd_start_offset, polygon_rdd_end_offset, polygon_rdd_splitter, carry_other_attributes)
```

If the data file is in TSV format, just simply use the following line to replace the old FileDataSplitter:
```python
polygon_rdd_splitter = FileDataSplitter.TSV
```

The way to create a LineStringRDD is the same as PolygonRDD.

### Create a generic SpatialRDD (behavoir changed in v1.2.0)

A generic SpatialRDD is not typed to a certain geometry type and open to more scenarios. It allows an input data file contains mixed types of geometries. For instance, a WKT file contains three types gemetries ==LineString==, ==Polygon== and ==MultiPolygon==.

#### From WKT/WKB
Geometries in a WKT and WKB file always occupy a single column no matter how many coordinates they have. Therefore, creating a typed SpatialRDD is easy.

Suppose we have a `checkin.tsv` WKT TSV file at Path `/Download/checkin.tsv` as follows:
```
POINT (-88.331492,32.324142)	hotel
POINT (-88.175933,32.360763)	gas
POINT (-88.388954,32.357073)	bar
POINT (-88.221102,32.35078)	restaurant
```
This file has two columns and corresponding ==offsets==(Column IDs) are 0, 1. Column 0 is the WKT string and Column 1 is the checkin business type.

Use the following code to create a SpatialRDD

```Scala
val inputLocation = "/Download/checkin.csv"
val wktColumn = 0 // The WKT string starts from Column 0
val allowTopologyInvalidGeometries = true // Optional
val skipSyntaxInvalidGeometries = false // Optional
val spatialRDD = WktReader.readToGeometryRDD(sparkSession.sparkContext, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
```

#### From GeoJSON

Geometries in GeoJSON is similar to WKT/WKB. However, a GeoJSON file must be beaked into multiple lines.

Suppose we have a `polygon.json` GeoJSON file at Path `/Download/polygon.json` as follows:

```
{ "type": "Feature", "properties": { "STATEFP": "01", "COUNTYFP": "077", "TRACTCE": "011501", "BLKGRPCE": "5", "AFFGEOID": "1500000US010770115015", "GEOID": "010770115015", "NAME": "5", "LSAD": "BG", "ALAND": 6844991, "AWATER": 32636 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -87.621765, 34.873444 ], [ -87.617535, 34.873369 ], [ -87.6123, 34.873337 ], [ -87.604049, 34.873303 ], [ -87.604033, 34.872316 ], [ -87.60415, 34.867502 ], [ -87.604218, 34.865687 ], [ -87.604409, 34.858537 ], [ -87.604018, 34.851336 ], [ -87.603716, 34.844829 ], [ -87.603696, 34.844307 ], [ -87.603673, 34.841884 ], [ -87.60372, 34.841003 ], [ -87.603879, 34.838423 ], [ -87.603888, 34.837682 ], [ -87.603889, 34.83763 ], [ -87.613127, 34.833938 ], [ -87.616451, 34.832699 ], [ -87.621041, 34.831431 ], [ -87.621056, 34.831526 ], [ -87.62112, 34.831925 ], [ -87.621603, 34.8352 ], [ -87.62158, 34.836087 ], [ -87.621383, 34.84329 ], [ -87.621359, 34.844438 ], [ -87.62129, 34.846387 ], [ -87.62119, 34.85053 ], [ -87.62144, 34.865379 ], [ -87.621765, 34.873444 ] ] ] } },
{ "type": "Feature", "properties": { "STATEFP": "01", "COUNTYFP": "045", "TRACTCE": "021102", "BLKGRPCE": "4", "AFFGEOID": "1500000US010450211024", "GEOID": "010450211024", "NAME": "4", "LSAD": "BG", "ALAND": 11360854, "AWATER": 0 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -85.719017, 31.297901 ], [ -85.715626, 31.305203 ], [ -85.714271, 31.307096 ], [ -85.69999, 31.307552 ], [ -85.697419, 31.307951 ], [ -85.675603, 31.31218 ], [ -85.672733, 31.312876 ], [ -85.672275, 31.311977 ], [ -85.67145, 31.310988 ], [ -85.670622, 31.309524 ], [ -85.670729, 31.307622 ], [ -85.669876, 31.30666 ], [ -85.669796, 31.306224 ], [ -85.670356, 31.306178 ], [ -85.671664, 31.305583 ], [ -85.67177, 31.305299 ], [ -85.671878, 31.302764 ], [ -85.671344, 31.302123 ], [ -85.668276, 31.302076 ], [ -85.66566, 31.30093 ], [ -85.665687, 31.30022 ], [ -85.669183, 31.297677 ], [ -85.668703, 31.295638 ], [ -85.671985, 31.29314 ], [ -85.677177, 31.288211 ], [ -85.678452, 31.286376 ], [ -85.679236, 31.28285 ], [ -85.679195, 31.281426 ], [ -85.676865, 31.281049 ], [ -85.674661, 31.28008 ], [ -85.674377, 31.27935 ], [ -85.675714, 31.276882 ], [ -85.677938, 31.275168 ], [ -85.680348, 31.276814 ], [ -85.684032, 31.278848 ], [ -85.684387, 31.279082 ], [ -85.692398, 31.283499 ], [ -85.705032, 31.289718 ], [ -85.706755, 31.290476 ], [ -85.718102, 31.295204 ], [ -85.719132, 31.29689 ], [ -85.719017, 31.297901 ] ] ] } },
{ "type": "Feature", "properties": { "STATEFP": "01", "COUNTYFP": "055", "TRACTCE": "001300", "BLKGRPCE": "3", "AFFGEOID": "1500000US010550013003", "GEOID": "010550013003", "NAME": "3", "LSAD": "BG", "ALAND": 1378742, "AWATER": 247387 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -86.000685, 34.00537 ], [ -85.998837, 34.009768 ], [ -85.998012, 34.010398 ], [ -85.987865, 34.005426 ], [ -85.986656, 34.004552 ], [ -85.985, 34.002659 ], [ -85.98851, 34.001502 ], [ -85.987567, 33.999488 ], [ -85.988666, 33.99913 ], [ -85.992568, 33.999131 ], [ -85.993144, 33.999714 ], [ -85.994876, 33.995153 ], [ -85.998823, 33.989548 ], [ -85.999925, 33.994237 ], [ -86.000616, 34.000028 ], [ -86.000685, 34.00537 ] ] ] } },
{ "type": "Feature", "properties": { "STATEFP": "01", "COUNTYFP": "089", "TRACTCE": "001700", "BLKGRPCE": "2", "AFFGEOID": "1500000US010890017002", "GEOID": "010890017002", "NAME": "2", "LSAD": "BG", "ALAND": 1040641, "AWATER": 0 }, "geometry": { "type": "Polygon", "coordinates": [ [ [ -86.574172, 34.727375 ], [ -86.562684, 34.727131 ], [ -86.562797, 34.723865 ], [ -86.562957, 34.723168 ], [ -86.562336, 34.719766 ], [ -86.557381, 34.719143 ], [ -86.557352, 34.718322 ], [ -86.559921, 34.717363 ], [ -86.564827, 34.718513 ], [ -86.567582, 34.718565 ], [ -86.570572, 34.718577 ], [ -86.573618, 34.719377 ], [ -86.574172, 34.727375 ] ] ] } },

```

Use the following code to create a generic SpatialRDD:
```python
input_location = "polygon.json"
allow_topology_invalid_geometries = True  # Optional
skip_syntax_invalid_geometries = True  # Optional
spatial_rdd = GeoJsonReader.readToGeometryRDD(sc, input_location, allow_topology_invalid_geometries, skip_syntax_invalid_geometries)
```

!!!warning
	The way that GeoSpark reads JSON file is different from SparkSQL
	
#### From Shapefile

```python
shape_file_input_location="/Download/myshapefile"
spatial_rdd = ShapefileReader.readToGeometryRDD(sc, shape_file_input_location)
```

!!!note
	The file extensions of .shp, .shx, .dbf must be in lowercase. Assume you have a shape file called ==myShapefile==, the file structure should be like this:
	```
	- shapefile1
	- shapefile2
	- myshapefile
		- myshapefile.shp
		- myshapefile.shx
		- myshapefile.dbf
		- myshapefile...
		- ...
	```

If the file you are reading contains non-ASCII characters you'll need to explicitly set the encoding
via `geospark.global.charset` system property before the call to `ShapefileReader.readToGeometryRDD`.

Example:

```Scala
System.setProperty("geospark.global.charset", "utf8")
```

#### From SparkSQL DataFrame

To create a generic SpatialRDD from CSV, TSV, WKT, WKB and GeoJSON input formats, you can use GeoSparkSQL. Make sure you include ==the full dependencies== of GeoSpark. Read [GeoSparkSQL API](../api/sql/GeoSparkSQL-Overview).

We use [checkin.csv CSV file](#pointrdd-from-csvtsv) as the example. You can create a generic SpatialRDD using the following steps:

1. Load data in GeoSparkSQL.
```python
df = spark.read.\
    format("csv").\
    option("delimiter", "\t").\
    option("header", "false").\
    load(csv_point_input_location)

df.createOrReplaceTempView("inputtable")

```
2. Create a Geometry type column in GeoSparkSQL
```python
spatial_df = spark.sql(
    """
        SELECT ST_Point(CAST(inputtable._c0 AS Decimal(24,20)),CAST(inputtable._c1 AS Decimal(24,20))) AS checkin
        FROM inputtable
    """
)
```
3. Use GeoSparkSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD
```python
from geo_pyspark.utils.adapter import Adapter

spatial_rdd = SpatialRDD()
spatial_rdd.rawJvmSpatialRDD = Adapter.toRdd(spatial_df)
```

For WKT/WKB/GeoJSON data, please use ==ST_GeomFromWKT / ST_GeomFromWKB / ST_GeomFromGeoJSON== instead.
	
## Transform the Coordinate Reference System

GeoSpark doesn't control the coordinate unit (degree-based or meter-based) of all geometries in an SpatialRDD. The unit of all related distances in GeoSpark is same as the unit of all geometries in an SpatialRDD.

To convert Coordinate Reference System of an SpatialRDD, use the following code:

```python
source_crs_code = "epsg:4326"  # WGS84, the most common degree-based CRS
target_crs_code = "epsg:3857"  # The most common meter-based CRS
object_rdd.CRSTransform(source_crs_code, target_crs_code)
```

!!!warning
	CRS transformation should be done right after creating each SpatialRDD, otherwise it will lead to wrong query results. For instace, use something like this:
	```python
	object_rdd = PointRDD(sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, carry_other_attributes)
	object_rdd.CRSTransform("epsg:4326", "epsg:3857")
	```

The details CRS information can be found on [EPSG.io](https://epsg.io/.)

## Read other attributes in an SpatialRDD

Each SpatialRDD can carry non-spatial attributes such as price, age and name as long as the user sets ==carryOtherAttributes== as [TRUE](#create-a-spatialrdd).

The other attributes are combined together to a string and stored in ==UserData== field of each geometry.

To retrieve the UserData field, use the following code:
```python
rdd_with_other_attributes = object_rdd.rawSpatialRDD.map(lambda x: x.getUserData())
``` 

## Write a Spatial Range Query

A spatial range query takes as input a range query window and an SpatialRDD and returns all geometries that intersect / are fully covered by the query window.


Assume you now have an SpatialRDD (typed or generic). You can use the following code to issue an Spatial Range Query on it.

```python
from geo_pyspark.core import Envelope
from geo_pyspark.core.spatialOperator import RangeQuery

range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
consider_boundary_intersection = False  ## Only return gemeotries fully covered by the window
using_index = False
query_result = RangeQuery.SpatialRangeQuery(spatial_rdd, range_query_window, consider_boundary_intersection, using_index)
```

==considerBoundaryIntersection== can be set to TRUE to return all geometries intersect with query window.

!!!note
	Spatial range query is equal to ==ST_Within== and ==ST_Intersects== in Spatial SQL. An example query is as follows:
	```SQL
	SELECT *
	FROM checkin
	WHERE ST_Intersects(queryWindow, checkin.location)
	```

### Range query window

Besides the rectangle (Envelope) type range query window, GeoSpark range query window can be Point/Polygon/LineString.

The code to create a point is as follows:

```python
from shapely.geometry import Point

point_object = Point(-84.01, 34.01)
```

The code to create a polygon (with 4 vertexes) is as follows:

```Scala
val geometryFactory = new GeometryFactory()
val coordinates = new Array[Coordinate](5)
coordinates(0) = new Coordinate(0,0)
coordinates(1) = new Coordinate(0,4)
coordinates(2) = new Coordinate(4,4)
coordinates(3) = new Coordinate(4,0)
coordinates(4) = coordinates(0) // The last coordinate is the same as the first coordinate in order to compose a closed ring
val polygonObject = geometryFactory.createPolygon(coordinates)
```

The code to create a line string (with 4 vertexes) is as follows:

```Scala
val geometryFactory = new GeometryFactory()
val coordinates = new Array[Coordinate](5)
coordinates(0) = new Coordinate(0,0)
coordinates(1) = new Coordinate(0,4)
coordinates(2) = new Coordinate(4,4)
coordinates(3) = new Coordinate(4,0)
val linestringObject = geometryFactory.createLineString(coordinates)
```

### Use spatial indexes

GeoSpark provides two types of spatial indexes, Quad-Tree and R-Tree. Once you specify an index type, GeoSpark will build a local tree index on each of the SpatialRDD partition.

To utilize a spatial index in a spatial range query, use the following code:

```Scala
val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
val considerBoundaryIntersection = false // Only return gemeotries fully covered by the window

val buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
spatialRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

val usingIndex = true
var queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, considerBoundaryIntersection, usingIndex)
```

!!!tip
	Using an index might not be the best choice all the time because building index also takes time. A spatial index is very useful when your data is complex polygons and line strings.

### Output format

The output format of the spatial range query is another SpatialRDD.

## Write a Spatial KNN Query

A spatial K Nearnest Neighbor query takes as input a K, a query point and an SpatialRDD and finds the K geometries in the RDD which are the closest to he query point.

Assume you now have an SpatialRDD (typed or generic). You can use the following code to issue an Spatial KNN Query on it.

```Scala
val geometryFactory = new GeometryFactory()
val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
val K = 1000 // K Nearest Neighbors
val usingIndex = false
val result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
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

Besides the Point type, GeoSpark KNN query center can be Polygon and LineString.

To learn how to create Polygon and LineString object, see [Range query window](#range-query-window).




### Use spatial indexes

To utilize a spatial index in a spatial KNN query, use the following code:

```Scala
val geometryFactory = new GeometryFactory()
val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
val K = 1000 // K Nearest Neighbors


val buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
spatialRDD.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)

val usingIndex = true
val result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
```

!!!warning
	Only R-Tree index supports Spatial KNN query

### Output format

The output format of the spatial KNN query is a list of geometries. The list has K geometry objects.

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

A distance join query takes as input two Spatial RDD A and B and a distance. For each geometry in A, finds the geometries (from B) are within the given distance to it. A and B can be any geometry type and are not necessary to have the same geometry type. The unit of the distance is explained [here](#transform-the-coordinate-reference-system).

Assume you now have two SpatialRDDs (typed or generic). You can use the following code to issue an Distance Join Query on them.

```Scala
objectRddA.analyze()

val circleRDD = new CircleRDD(objectRddA, 0.1) // Create a CircleRDD using the given distance

circleRDD.spatialPartitioning(GridType.KDBTREE)
objectRddB.spatialPartitioning(circleRDD.getPartitioner)

val considerBoundaryIntersection = false // Only return gemeotries fully covered by each query window in queryWindowRDD
val usingIndex = false

val result = JoinQuery.DistanceJoinQueryFlat(objectRddB, circleRDD, usingIndex, considerBoundaryIntersection)
```

The rest part of the join query is same as the spatial join query.

The details of spatial partitioning in join query is [here](#use-spatial-partitioning).

The details of using spatial indexes in join query is [here](#use-spatial-indexes_2).

The output format of the distance join query is [here](#output-format_2).

!!!note
	Distance join query is equal to the following query in Spatial SQL:
	```SQL
	SELECT superhero.name
	FROM city, superhero
	WHERE ST_Distance(city.geom, superhero.geom) <= 10;
	```
	Find the super heros within 10 miles of each city
	
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

```Scala
objectRDD.rawSpatialRDD.saveAsObjectFile("hdfs://PATH")
```

!!!note
	Each object in a distributed object file is a byte array (not human-readable). This byte array is the serialized format of a Geometry or a SpatialIndex.

### Save an SpatialRDD (indexed)

Indexed typed SpatialRDD and generic SpatialRDD can be saved to permanent storage. However, the indexed SpatialRDD has to be stored as a distributed object file.

#### Save to distributed object file

Use the following code to save an SpatialRDD as a distributed object file:

```
objectRDD.indexedRawRDD.saveAsObjectFile("hdfs://PATH")
```

### Save an SpatialRDD (spatialPartitioned W/O indexed)

A spatial partitioned RDD can be saved to permanent storage but Spark is not able to maintain the same RDD partition Id of the original RDD. This will lead to wrong join query results. We are working on some solutions. Stay tuned!

### Reload a saved SpatialRDD

You can easily reload an SpatialRDD that has been saved to ==a distributed object file==.

#### Load to a typed SpatialRDD

Use the following code to reload the PointRDD/PolygonRDD/LineStringRDD:

```Scala
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
