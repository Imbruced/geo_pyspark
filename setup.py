from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))


setup(
    name='geo_pyspark',
    version='0.1.0',
    description='GeoSpark Python Wrapper',
    url='https://github.com/Imbruced/geo_pyspark',
    author='Pawel Kocinski',
    author_email='pawel93kocinski@gmail.com',
    packages=['geo_pyspark'],
    python_requires='>=3.6',
    install_requires=['pyspark', 'findspark', 'pandas', 'geopandas'],
    project_urls={
        'Bug Reports': 'https://github.com/Imbruced/geo_pyspark'
    },
)