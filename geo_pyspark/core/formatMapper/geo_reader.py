from abc import ABC
import abc

import attr
from pyspark import SparkContext


@attr.s
class GeoDataReader(ABC):

    @abc.abstractmethod
    def readToGeometryRDD(cls):
        raise NotImplementedError(f"Instance of the class {cls.__class__.__name__} has to implement method readToGeometryRDD")