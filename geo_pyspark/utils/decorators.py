from typing import List, Iterable, Callable, TypeVar

from geo_pyspark.register.java_libs import GeoSparkLib

T = TypeVar('T')


class classproperty(object):

    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


def get_first_meet_criteria_element_from_iterable(iterable: Iterable[T], criteria: Callable[[T], int]) -> int:
    for index, element in enumerate(iterable):
        if criteria(element):
            return index
    return -1


def require(library_names: List[GeoSparkLib]):
    def wrapper(func):
        def run_function(*args, **kwargs):
            from geo_pyspark.core.utils import ImportedJvmLib
            has_all_libs = [lib for lib in library_names]
            first_not_fulfill_value = get_first_meet_criteria_element_from_iterable(
                has_all_libs, lambda x: not ImportedJvmLib.has_library(x)
            )

            if first_not_fulfill_value == -1:
                return func(*args, **kwargs)
            else:
                raise ModuleNotFoundError(f"Did not found {has_all_libs[first_not_fulfill_value]}, make sure that was correctly imported via py4j"
                                          f"Did you use GeoSparkRegistrator.registerAll ? ")
        return run_function
    return wrapper

