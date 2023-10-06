from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


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


def working_with_schemas():
    pass
