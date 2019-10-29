.. geo_pyspark documentation master file, created by
   sphinx-quickstart on Sat Oct 12 19:27:59 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. include installation.rst
.. include examples.rst
.. include supported_versions


Welcome to geo_pyspark documentation!
=======================================
Welcome to Documentation.
Contents:



=================
Introduction
=================

Package is a Python wrapper on scala library GeoSparkSQL. Official repository for GeoSpark can be found at https://github.com/DataSystemsLab/GeoSpark.

Package allow to use all GeoSparkSQL functions and transform it to Python Shapely geometry objects. Also it allows to create Spark DataFrame with GeoSpark UDT from Shapely geometry objects. Spark DataFrame can be converted to GeoPandas easily, in addition all fiona drivers for shape file are available to load data from files and convert them to Spark DataFrame. Please look at examples.


=================
Installation
=================

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

.. code-block:: python

  from pyspark.sql import SparkSession

  from geo_pyspark.register import upload_jars
  from geo_pyspark.register import GeoSparkRegistrator

  upload_jars()

  spark = SparkSession.builder.\
        getOrCreate()

  GeoSparkRegistrator.registerAll(spark)


Function 

.. code-block:: python

  upload_jars()

uses findspark Python package to upload jar files to executor and nodes. To avoid copying all the time, jar files can be put in directory $SPARK_HOME/jars or any other path specified in Spark config files.



Installing from wheel file
--------------------------

.. code-block:: bash

  pipenv run python -m pip install dist/geo_pyspark-0.2.0-py3-none-any.whl

or

.. code-block:: bash

  pip install dist/geo_pyspark-0.2.0-py3-none-any.whl


Installing from source
----------------------

.. code-block:: bash

  python3 setup.py install



=================
Examples
=================

=======================================
Integration with GeoPandas and Shapely
=======================================

geo_pyspark has implemented serializers and deserializers which allows to convert GeoSpark Geometry objects into Shapely BaseGeometry objects. Based on that it is possible to load the data with geopandas from file (look at Fiona possible drivers) and create Spark DataFrame based on GeoDataFrame object. 

Example, loading the data from shapefile using geopandas read_file method and create Spark DataFrame based on GeoDataFrame:

.. code-block:: python

  import os

  import geopandas as gpd
  from pyspark.sql import SparkSession

  from geo_pyspark.data import data_path
  from geo_pyspark.register import GeoSparkRegistrator

  spark = SparkSession.builder.\
        getOrCreate()

  GeoSparkRegistrator.registerAll(spark)

  gdf = gpd.read_file("gis_osm_pois_free_1.shp")

  spark.createDataFrame(
    gdf
  ).show()


.. code-block:: python

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



Reading data with Spark and converting to GeoPandas

.. code-block:: python

  import os

  import geopandas as gpd
  from pyspark.sql import SparkSession

  from geo_pyspark.data import data_path
  from geo_pyspark.register import GeoSparkRegistrator

  spark = SparkSession.builder.\
        getOrCreate()

  GeoSparkRegistrator.registerAll(spark)

  counties = spark.\
    read.\
    option("delimiter", "|").\
    option("header", "true").\
    csv(os.path.join(data_path, "counties.csv"))
      
  counties.createOrReplaceTempView("county")

  counties_geom = spark.sql(
          "SELECT *, st_geomFromWKT(geom) as geometry from county"
  )

  df = counties_geom.toPandas()
  gdf = gpd.GeoDataFrame(df, geometry="geometry")
  gdf.plot()



==================
Supported versions
==================

Apache Spark
------------

Currently package supports spark versions

- 2.2
- 2.3
- 2.4


GeoSpark
--------

- 1.2.0
- 1.1.3


.. toctree::
   :maxdepth: 2


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

