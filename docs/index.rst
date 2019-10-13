.. geo_pyspark documentation master file, created by
   sphinx-quickstart on Sat Oct 12 19:27:59 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to geo_pyspark documentation!
=======================================
Welcome to Documentation.
Contents:

.. include installation.rst
.. include examples.rst
.. include supported_versions


=================
Introduction
=================

Package is a Python wrapper on scala library GeoSparkSQL. Official repository for GeoSpark can be found at https://github.com/DataSystemsLab/GeoSpark.

Package allow to use all GeoSparkSQL functions and transform it to Python Shapely geometry objects. Also it allows to create Spark DataFrame with GeoSpark UDT from Shapely geometry objects. Spark DataFrame can be converted to GeoPandas easily, in addition all fiona drivers for shape file are available to load data from files and convert them to Spark DataFrame. Please look at examples.


=================
Installation
=================

=================
Examples
=================

=================
Integration with GeoPandas and Shapely
=================

=================
Supported versions
=================


.. toctree::
   :maxdepth: 2
 

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
