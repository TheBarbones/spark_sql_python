from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

"""
Data ingestion & Schema evolution
---------------------------------
    ✨ Loading data from various sources
    ✨ Handing schema evolution and data type inference
    ✨ Schema customization and enforcement


"""


def loading_data(spark: SparkSession):
    """
    - CSV or TEXT
    - JSON
    - PARQUET
    - ORC

    Options:
        - header
        - inferSchema
        - delimiter
        - encoding
        - quote
        - escape
        - multiLine
        - ignoreLeadingWhiteSpace
        - ignoreTrailingWhiteSpace




    :param spark:
    :return:
    """
    parm_path = "course/data/input/"
    csv_df = spark.read.option("header", "true").csv(f"{parm_path}iris_2023.xls")
    json_df = spark.read.option("header", "true").json(f"{parm_path}iris_2023.json")
    parquet_df = spark.read.option("header", "true").parquet(
        f"{parm_path}iris_2023.parquet"
    )

    return [csv_df, json_df, parquet_df]


def data_ingestion_schema_evolution(spark: SparkSession):
    list_input = loading_data(spark)

    csv_df = list_input[0]
    json_df = list_input[1]
    parquet_df = list_input[2]
