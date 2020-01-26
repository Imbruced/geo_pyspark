import itertools
import os
import shutil

from geo_pyspark import version

geo_wrapper_sbt = """
name := "geo_wrapper"

version := "{geo_pyspark_version}"

scalaVersion := "{scala_version}"

val SparkVersion = "{spark_version}"

val SparkCompatibleVersion = "{spark_compatible_version}"

val GeoSparkVersion = "{geo_spark_version}"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % SparkVersion,
  "org.datasyslab" % "geospark" % GeoSparkVersion,
  "org.datasyslab" % "geospark-sql_".concat(SparkCompatibleVersion) % GeoSparkVersion ,
  "org.datasyslab" % "geospark-viz_".concat(SparkCompatibleVersion) % GeoSparkVersion
)

"""
pipfile = """
[[source]]
name = "pypi"
url = "https://pypi.org/simple"
verify_ssl = true

[dev-packages]
matplotlib="*"
descartes="*"
sphinx="*"
ipykernel="*"
jupyter="*"
toree="*"
pytest="*"
mypy="*"
pytest-json-report="*"

[packages]
findspark="*"
pandas="*"
geopandas="=={geopandas_version}"
pyspark="==2.4.4"

[requires]
python_version = "{python_version}"
"""

testing_libraries = {
    "scala_version": ["2.11.8"],
    "spark_version_and_compatible_and_geospark_version": [
        ("2.4.4", "2.3", "1.2.0"), ("2.3.4", "2.3", "1.2.0"), ("2.2.3", "2.2", "1.2.0"),
        ("2.4.4", "2.3", "1.1.3"), ("2.3.4", "2.3", "1.1.3"), ("2.2.3", "2.2", "1.1.3")
    ],
    "geopandas": ["0.6.0"],
    "python": ["3.8"]
}

combinations = list(itertools.product(*testing_libraries.values()))
commands = []
geo_pyspark_location = "/home/pawel/Desktop/geo_pyspark"
geo_wrapper_location = os.path.join(geo_pyspark_location, "geo_wrapper")
os.environ["SBT_HOME"] = "/usr/sbt"
ci_location = os.path.join(geo_pyspark_location, "ci")
ci_jars_location = os.path.join(ci_location, "jars")
ci_geo_spark_location = os.path.join(ci_jars_location, "geo_spark")
ci_geo_spark_location_sql = os.path.join(ci_jars_location, "geo_spark_sql")

for scala_version, (spark_version, spark_compatible_version, geo_spark_version), geo_pandas_version, python_version in combinations:
    print(
        f"scala_version: {scala_version} geo_spark_version: {geo_spark_version} spark_version: {spark_version} geo_pandas_version: {geo_pandas_version} python_version: {python_version}"
    )
    pip_file_command = pipfile.format(
                geopandas_version=geo_pandas_version,
                python_version=python_version
            ).strip()

    sbt_command = geo_wrapper_sbt.format(
                geo_pyspark_version=version,
                scala_version=scala_version,
                spark_version=spark_version,
                spark_compatible_version=spark_compatible_version,
                geo_spark_version=geo_spark_version,
            ).strip()

    with open(os.path.join(geo_pyspark_location, "Pipfile"), "w") as file:
        file.write(pip_file_command)
    with open(os.path.join(geo_wrapper_location, "build.sbt"), "w") as file:
        file.write(sbt_command)

    os.system(f"cd {geo_wrapper_location} ; {os.environ['SBT_HOME']}/bin/sbt package")
    scala_main_version = ".".join(scala_version.split(".")[:-1])

    wrapper_jar_location = f"{geo_wrapper_location}/target/scala-{scala_main_version}/geo_wrapper_{scala_main_version}-{version}.jar"
    geo_spark_core_jar_location = f"{ci_geo_spark_location}/geospark-{geo_spark_version}.jar"
    geo_spark_sql_jar_location = f"{ci_geo_spark_location_sql}/geospark-sql_{spark_compatible_version}-{geo_spark_version}.jar"

    spark_home = os.path.join(ci_jars_location, "spark", f"spark-{spark_version}-bin-hadoop2.7")
    os.environ["SPARK_HOME"] = spark_home
    lib_files = os.listdir(f"{os.environ['SPARK_HOME']}/python/lib/")
    py4j_version = [file for file in lib_files if "py4j" in file][0]

    os.environ["PYTHONPATH"] = os.environ["SPARK_HOME"] + f"/python:{os.environ['SPARK_HOME']}/python/lib/{py4j_version}"
    spark_jars_location = os.path.join(spark_home, "jars")

    os.system(f"cp  {wrapper_jar_location} {spark_jars_location}")
    os.system(f"cp  {geo_spark_core_jar_location} {spark_jars_location}")
    os.system(f"cp  {geo_spark_sql_jar_location} {spark_jars_location}")
    os.system(f"/bin/bash {geo_pyspark_location}/build_wheel.sh")
    os.system("pipenv --rm")
    os.system("pipenv lock --clear")
    os.system("pipenv install -d")
    os.system(f"pipenv install {geo_pyspark_location}/dist/geo_pyspark-{version}-py3-none-any.whl")
    os.system(f"pipenv install pytest-json-report")
    os.system(f"pipenv run pytest -v {geo_pyspark_location}/tests/spatial_operator/test_join_query_correctness.py --json-report --json-report-file={ci_location}/python_{python_version.replace('.', '_')}_spark_{spark_version.replace('.', '_')}_geo_spark{geo_spark_version.replace('.', '_')} {geo_pyspark_location}/tests")
    jars_home = os.path.join(spark_home, "jars/")

    jar_files = os.listdir(jars_home)
    geo_jars = [os.path.join(jars_home, file) for file in jar_files if "geo" in file]
    for geo_file in geo_jars:
        if os.path.exists(geo_file):
            os.remove(geo_file)
