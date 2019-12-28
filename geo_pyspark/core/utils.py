from typing import Optional, List, Iterable, Callable, TypeVar

import attr
from pyspark import SparkContext

from geo_pyspark.register.java_libs import GeoSparkLib
from geo_pyspark.utils.decorators import classproperty


T = TypeVar('T')


class ImportedJvmLib:
    _imported_libs = []

    @classmethod
    def has_library(cls, library: GeoSparkLib) -> bool:
        return library in cls._imported_libs

    @classmethod
    def import_lib(cls, library: str) -> bool:
        if library not in cls._imported_libs:
            cls._imported_libs.append(library)
        else:
            return False
        return True


def get_first_meet_criteria_element_from_iterable(iterable: Iterable[T], criteria: Callable[[T], int]) -> int:
    for index, element in enumerate(iterable):
        if criteria(element):
            return index
    return -1


def require(library_names: List[GeoSparkLib]):
    def wrapper(func):
        def run_function(*args, **kwargs):
            has_all_libs = [lib for lib in library_names]
            first_not_fulfill_value = get_first_meet_criteria_element_from_iterable(
                has_all_libs, lambda x: not ImportedJvmLib.has_library(x)
            )

            if first_not_fulfill_value == -1:
                return func(*args, **kwargs)
            else:
                raise ModuleNotFoundError(f"Did not found {has_all_libs[first_not_fulfill_value]}, make sure that was correctly imported via py4j")
        return run_function
    return wrapper


