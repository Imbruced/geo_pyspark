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

## Initiate SparkContext

```python
from pyspark.sql import SparkSession

from geo_pyspark.utils import KryoSerializer, GeoSparkKryoRegistrator

spark = SparkSession. \
    builder. \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName). \
    master("local[*]"). \
    getOrCreate()

sc = spark.sparkContext
```

!!!warning
	GeoSpark has a suite of well-written geometry and index serializers. Forgetting to enable these serializers will lead to high memory consumption.

If you add ==the GeoSpark full dependencies== as suggested above, please use the following two lines to enable GeoSpark Kryo serializer instead:

# Create a SpatialRDD

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

point_rdd_input_location = "/Download/checkin.csv"
point_rdd_offset = 0  ## The point long/lat starts from Column 0
point_rdd_splitter = FileDataSplitter.CSV
carry_other_attributes = True  ## Carry Column 2 (hotel, gas, bar...)
object_rdd = PointRDD(sc, point_rdd_input_location, point_rdd_offset, point_rdd_splitter, carry_other_attributes)
```