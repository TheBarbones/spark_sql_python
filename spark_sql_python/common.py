from pyspark.sql import SparkSession, Row
from spark_sql_python.config import generic_settings
import json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
)


def read_file_with_schema(
    spark: SparkSession,
    file_name: str,
    file_type: str,
    schema_file: str | None = None,
):
    parm_path = generic_settings.energy_data_path
    parm_folder = generic_settings.raw_folder
    file = f"{parm_path}{parm_folder}{file_name}.{file_type}"

    ##refactoring apply
    schema = StructType([StructField("column", StringType(), True)])
    initial_df = spark.createDataFrame([], schema)

    match file_type:
        case generic_settings.csv_type:
            initial_df = spark.read.schema(schema_file).csv(file)
        case generic_settings.json_type:
            initial_df = spark.read.schema(schema_file).json(file)
        case generic_settings.parquet_type:
            initial_df = spark.read.schema(schema_file).parquet(file)
    return initial_df


def get_schema_file(file_name: str):
    parm_path = generic_settings.energy_data_path
    parm_folder = generic_settings.schema_folder
    file = f"{parm_path}{parm_folder}{file_name}.{generic_settings.json_type}"
    with open(file) as f:
        json_file = f.read()
    schema = json.loads(json_file)
    return StructType.fromJson(schema)


def empty_dataframe(spark: SparkSession):
    schema = StructType([StructField("column", StringType(), True)])
    return spark.createDataFrame([], schema)
