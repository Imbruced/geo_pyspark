# geo_pyspark

GeoSpark python bindings.
Documentation in sphinx will be ready soon.

## Introduction

Package is a Python wrapper on scala library <b>GeoSparkSQL</b>. Official repository for GeoSpark can be found at
https://github.com/DataSystemsLab/GeoSpark. 

Package allow to use all GeoSparkSQL functions and transform it to Python Shapely geometry objects. Also
it allows to create Spark DataFrame with GeoSpark UDT from Shapely geometry objects. Spark DataFrame can
be converted to GeoPandas easily, in addition all fiona drivers for shape file are available to load
data from files and convert them to Spark DataFrame. Please look at examples.

## Instalation

### pipenv

clone repository

```bash
git clone https://github.com/Imbruced/geo_pyspark.git
```
Go into directory and run to install all dependencies if you want to create env from
scratch
```python
pipenv install 
pipenv shell
```

Install package from wheel file
```python
pipenv run python -m pip install dist/geo_pyspark-0.1.0-py3-none-any.whl
```
Or using setup.py file
```python
pipenv run python setup.py install
```

### pip

clone repository

```bash
git clone https://github.com/Imbruced/geo_pyspark.git
```

And install package from wheel file

```python
pip install dist/geo_pyspark-0.1.0-py3-none-any.whl
```

### bare python

clone repository

```bash
git clone https://github.com/Imbruced/geo_pyspark.git
```

```python
python setup.py install
```

## Example usage


example:

```python
from pyspark.sql import SparkSession
from geo_pyspark.register import GeoSparkRegistrator


spark = SparkSession.builder.\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)

df = spark.sql("""SELECT st_geomfromwkt('POINT(6.0 52.0)') as geom""")

df.show()

```
    +------------+
    |        geom|
    +------------+
    |POINT (6 52)|
    +------------+

##