from pyspark.sql.functions import lit, col
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from course.config.sparkSession import spark, loading_data

"""
DataFrame API vs. Spark SQL
Spark SQL:
    - Work with structure (Csv, Parquet, etc) and semi-structured data (Json, Xml)
    - Support SQL-like sintax or SQL ANSI
    - Work with Spark's distributed computing capabilities
    - Provide interface to work with MLib, GraphX and Streaming

Dataframe API:
    - Interface more friendly than RDD API
    - Operations similar than SQL
    - Support wide range of data formats: Csv, Parquet, Json, Orc, etc
    
Differences:
    - Execution engine: 
        - Spark SQL: Uses a query optimizer and an execution engine
        - Dataframe API: Relies on Spark's RDD execution engine
    - Code generation:
        - Spark SQL: Generate bytecode at runtime
        - Dataframe API: Relies on the JVM
    - SQL-like syntax:
        - Spark SQL: Syntax with SQL-like, similar to SQL ANSI
        - Dataframe API: Functional programming constructs
        
Basic sql operations
    - Registering Dataframes as tables
    - Running SQL Queries
    - Aggregations
    - Filtering and Sorting
    - Joining Data
    - Writing Data
    - Caching Data
    - Stopping SparkSession
"""


def registering_dataframes_as_tables(list_input: list[DataFrame]):
    """

    :param list_input:
    :return:
    """
    list_input[0].createTempView("csv_view")
    list_input[1].createTempView("json_view")
    list_input[2].createTempView("parquet_view")


def running_sql_queries(spark: SparkSession):
    """

    :param spark:
    :return:
    """
    csv_df = spark.sql("select * from csv_view")
    json_df = spark.sql("select * from json_view")
    parquet_df = spark.sql("select * from parquet_view")

    csv_df.show()
    json_df.show()
    parquet_df.show()


def aggregations(list_input: list[DataFrame]):
    """

    :param list_input:
    :return:
    """
    parquet_df = list_input[2]
    agg_df = parquet_df.groupby(col("species")).agg(
        {
            "sepal_length": "max",
            "sepal_width": "min",
            "petal_length": "count",
            "petal_width": "avg",
        }
    )


def filtering_and_sorting(list_input: list[DataFrame]):
    """

    :param list_input:
    :return:
    """
    parquet_df = list_input[2]
    filtered_df = parquet_df.filter(col("species") == lit("setosa"))


def joining_data(list_input: list[DataFrame]):
    """

    :param list_input:
    :return:
    """
    joining_one_df = list_input[0]
    joining_two_df = list_input[2]

    joining_data_df = joining_one_df.alias("j1").join(
        joining_two_df.alias("j2"), col("j1.id") == col("j2.id"), "inner"
    )

    joining_data_df = joining_one_df.join(joining_two_df, ["id"], "inner")

    condition = [col("j1.id") == col("j2.id")]
    joining_data_df = joining_one_df.alias("j1").join(
        joining_two_df.alias("j2"), condition, "left"
    )


def writing_data(list_input: list[DataFrame]):
    """

    :param list_input:
    :return:
    """
    writing_df = list_input[2]
    writing_df.write.mode("overwrite").parquet("course/data/output/write_exercise/")


def caching_data(list_input: list[DataFrame]):
    """

    :param list_input:
    :return:
    """
    caching_df = list_input[2]
    caching_df.cache()


loading_data_list = loading_data(spark)
registering_dataframes_as_tables(loading_data_list)
running_sql_queries(spark)
aggregations(loading_data_list)
filtering_and_sorting(loading_data_list)
joining_data(loading_data_list)
writing_data(loading_data_list)
caching_data(loading_data_list)
