import os
from typing import Tuple, List

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))
jars_relative_path = "geo_pyspark/jars"


def create_data_files() -> List[Tuple[str, List[str]]]:
    data_files = []
    jars_path = os.path.join(here, jars_relative_path)
    for version in os.listdir(jars_path):
        version_path = os.path.join(jars_path, version)
        jar_files = [os.path.join(jars_relative_path, version, jar_file) for jar_file in os.listdir(version_path)]
        print(jar_files)
        data_files.append((os.path.join(jars_relative_path, version), jar_files))
    return data_files


setup(
    name='geo_pyspark',
    version='0.1.0',
    description='GeoSpark Python Wrapper',
    url='https://github.com/Imbruced/geo_pyspark',
    author='Pawel Kocinski',
    author_email='pawel93kocinski@gmail.com',
    packages=find_packages(exclude=['geo_pyspark.data', 'geo_pyspark.tests']),
    python_requires='>=3.6',
    install_requires=['pyspark', 'findspark', 'pandas', 'geopandas'],
    project_urls={
        'Bug Reports': 'https://github.com/Imbruced/geo_pyspark'
    },
    data_files=create_data_files()
)