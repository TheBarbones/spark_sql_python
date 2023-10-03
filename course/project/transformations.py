from pyspark.sql.functions import lit, col
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from course.config.sparkSession import spark


"""
Basic sql operations
    - Creating a SparkSession
    - Loading Data
    - Registering Dataframes as tables
    - Running SQL Queries
    - Aggregations
    - Filtering and Sorting
    - Joining Data
    - Writing Data
    - Caching Data
    - Stopping SparkSession
"""


def loading_data(spark: SparkSession):
    """Loading files:
    - CSV or TEXT
    - JSON
    - PARQUET
    - ORC
    """
    csv_df = spark.read.option("header", "true").csv("course/data/input/iris_2023.xls")
    json_df = spark.read.option("header", "true").json(
        "course/data/input/iris_2023.json"
    )
    parquet_df = spark.read.option("header", "true").parquet(
        "course/data/input/iris_2023.parquet"
    )

    return [csv_df, json_df, parquet_df]


def registering_dataframes_as_tables(list_input: list[DataFrame]):
    list_input[0].createTempView("csv_view")
    list_input[1].createTempView("json_view")
    list_input[2].createTempView("parquet_view")


def running_sql_queries(spark: SparkSession):
    csv_df = spark.sql("select * from csv_view")
    json_df = spark.sql("select * from json_view")
    parquet_df = spark.sql("select * from parquet_view")

    csv_df.show()
    json_df.show()
    parquet_df.show()


def aggregations(list_input: list[DataFrame]):
    parquet_df = list_input[2]
    agg_df = parquet_df.groupby(col("species")).agg(
        {
            "species": "max",
            "species": "min",
        }
    )

    agg_df.show()


def filtering_and_sorting(list_input: list[DataFrame]):
    pass


def joining_data(list_input: list[DataFrame]):
    pass


def writing_data(list_input: list[DataFrame]):
    pass


def caching_data(list_input: list[DataFrame]):
    pass


loading_data_list = loading_data(spark)
registering_dataframes_as_tables(loading_data_list)
running_sql_queries(spark)
aggregations(loading_data_list)
filtering_and_sorting(loading_data_list)
joining_data(loading_data_list)
writing_data(loading_data_list)
caching_data(loading_data_list)