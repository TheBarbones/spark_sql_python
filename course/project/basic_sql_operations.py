from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from course.config.sparkSession import spark, loading_data


"""
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
    Create a temporary view in memory.

    :param list_input: list of dataframes
    :return: None
    """
    list_input[0].createTempView("csv_view")
    list_input[1].createTempView("json_view")
    list_input[2].createTempView("parquet_view")


def running_sql_queries(spark: SparkSession):
    """
    Execution generic sql sentences

    :param spark: Spark session
    :return: None
    """
    csv_df = spark.sql("select * from csv_view")
    json_df = spark.sql("select * from json_view")
    parquet_df = spark.sql("select * from parquet_view")

    csv_df.show()
    json_df.show()
    parquet_df.show()


def aggregations(spark: DataFrame):
    """
    Aggregation operation to find avg, min, max, sum, count and others

    :param spark: Spark session
    :return: None
    """
    agg_df = spark.sql("SELECT count(*), species FROM csv_view GROUP BY species")
    agg_df.show()


def filtering_and_sorting(spark: DataFrame):
    """
    Filtering operations to get accurated values as much as we need
    Sorting operation to order our result

    :param spark: Spark session
    :return: None
    """
    filtered_df = spark.sql("SELECT * FROM csv_view WHERE species != 'setosa'")
    filtered_df.show()

    sorting_df = spark.sql(
        "SELECT * FROM csv_view WHERE species != 'setosa' ORDER BY id"
    )
    sorting_df.show()


def joining_data(spark: DataFrame):
    """
    Joinin operations such inner, left, right and others

    :param spark: Spark session
    :return: None
    """
    joining_df = spark.sql(
        "SELECT * FROM csv_view "
        "JOIN parquet_view ON csv_view.id = parquet_view.id "
        "WHERE parquet_view.species != 'setosa' ORDER BY csv_view.id"
    )

    joining_df.show()


def writing_data(spark: DataFrame):
    """
    Write operations and options

    :param spark: Spark session
    :return: None
    """
    writing_df = spark.sql("SELECT id, species FROM csv_view")
    writing_df.write.mode("overwrite").parquet("course/data/output/write_exercise/")


def caching_data(spark: DataFrame):
    """
    Caching data to improve the performance and

    :param spark: Spark session
    :return: None
    """
    caching_df = spark.sql("SELECT * FROM csv_view")
    caching_df.cache()


loading_data_list = loading_data(spark)
registering_dataframes_as_tables(loading_data_list)
running_sql_queries(spark)
aggregations(spark)
filtering_and_sorting(spark)
joining_data(spark)
writing_data(spark)
caching_data(spark)
