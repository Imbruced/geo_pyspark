# geo_pyspark

GeoSpark python bindings, lib is not ready yet, wait for updates.

All functionality of GeoSpark sql is available,
Collect is working for all geometry types, but for other than Point it deserializers has to be written
Goal is to convert them to shapely geometry objects and simplify convert to geopandas.
Serializers are not written yet.