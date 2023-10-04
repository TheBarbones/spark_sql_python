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


def aggregations(spark: DataFrame, list_input: list[DataFrame]):
    """

    :param list_input:
    :return:
    """
    # Spark SQL
    agg_df = spark.sql("SELECT count(*), species FROM csv_view GROUP BY species")
    agg_df.show()

    # Dataframe API
    parquet_df = list_input[2]
    agg_df = parquet_df.groupby(col("species")).agg(
        {
            "sepal_length": "max",
            "sepal_width": "min",
            "petal_length": "count",
            "petal_width": "avg",
        }
    )


def filtering_and_sorting(spark: DataFrame, list_input: list[DataFrame]):
    """

    :param list_input:
    :return:
    """
    # Spark SQL
    filtered_df = spark.sql("SELECT * FROM csv_view WHERE species != 'setosa'")
    filtered_df.show()

    sorting_df = spark.sql(
        "SELECT * FROM csv_view WHERE species != 'setosa' ORDER BY id"
    )
    sorting_df.show()

    # Dataframe API
    parquet_df = list_input[2]
    filtered_df = parquet_df.filter(col("species") == lit("setosa"))


def joining_data(spark: DataFrame, list_input: list[DataFrame]):
    """

    :param list_input:
    :return:
    """
    # Spark SQL
    joining_df = spark.sql(
        "SELECT * FROM csv_view "
        "JOIN parquet_view ON csv_view.id = parquet_view.id "
        "WHERE parquet_view.species != 'setosa' ORDER BY csv_view.id"
    )

    joining_df.show()

    # Dataframe API
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


loading_data_list = loading_data(spark)
registering_dataframes_as_tables(loading_data_list)
running_sql_queries(spark)
aggregations(spark, loading_data_list)
filtering_and_sorting(spark, loading_data_list)
joining_data(spark, loading_data_list)
