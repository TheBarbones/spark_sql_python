from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()

"""
Basic sql operations
    - Creating a SparkSession
    - Loading Data
"""


def loading_data(spark: SparkSession):
    """
    - CSV or TEXT
    - JSON
    - PARQUET
    - ORC
    :param spark:
    :return:
    """
    csv_df = spark.read.option("header", "true").csv("course/data/input/iris_2023.xls")
    json_df = spark.read.option("header", "true").json(
        "course/data/input/iris_2023.json"
    )
    parquet_df = spark.read.option("header", "true").parquet(
        "course/data/input/iris_2023.parquet"
    )

    return [csv_df, json_df, parquet_df]
